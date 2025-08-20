from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    rand, col, expr, when, monotonically_increasing_id,
    year, month, dayofmonth, to_timestamp, lit, hash as spark_hash, pmod
)
import requests
import base64
import os
import shutil
from datetime import datetime, timedelta

# === GitHub Configuration ===
GITHUB_TOKEN = dbutils.secrets.get(scope="databricksazure", key="github-pat-token")
GITHUB_REPO = "fusionoasis/analytics-data"
GITHUB_API_URL = f"https://api.github.com/repos/{GITHUB_REPO}/contents"

spark = SparkSession.builder.getOrCreate()

# === GitHub Upload Helpers ===
def get_last_index_from_github(github_folder):
    """Check the highest part number in GitHub folder"""
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }
    response = requests.get(f"{GITHUB_API_URL}/{github_folder}", headers=headers)

    if response.status_code != 200:
        return -1

    files = [item["name"] for item in response.json() if item["name"].startswith("part-")]
    if not files:
        return -1

    indices = []
    for f in files:
        try:
            idx = int(f.split("-")[1])
            indices.append(idx)
        except:
            continue
    return max(indices) if indices else -1

def upload_to_github(file_path, github_path, commit_message):
    """Create or update a single file in the GitHub repository via Contents API.

    If the file already exists, include its SHA to perform an update; otherwise create it.
    """
    local_path = file_path.replace("dbfs:", "/dbfs")
    with open(local_path, "rb") as f:
        content = f.read()
    content_encoded = base64.b64encode(content).decode()

    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }

    # Try to get existing file SHA (required for updates)
    sha = None
    meta_resp = requests.get(f"{GITHUB_API_URL}/{github_path}", headers=headers)
    if meta_resp.status_code == 200:
        try:
            sha = meta_resp.json().get("sha")
        except Exception:
            sha = None

    payload = {"message": commit_message, "content": content_encoded}
    if sha:
        payload["sha"] = sha

    response = requests.put(f"{GITHUB_API_URL}/{github_path}", json=payload, headers=headers)
    return response.status_code in [200, 201]

def get_last_index_from_local(local_folder: str) -> int:
    """Read local DBFS-mounted folder and find highest index in files named part-00000-tid.*"""
    if not os.path.isdir(local_folder):
        return -1
    max_idx = -1
    for name in os.listdir(local_folder):
        if not name.startswith("part-"):
            continue
        try:
            core = name.split("-")[1]  # 00000
            idx = int(core)
            max_idx = max(max_idx, idx)
        except Exception:
            continue
    return max_idx


def upload_file_with_github_index(local_file_path: str, github_folder: str, commit_msg: str) -> bool:
    """Upload the given local file to GitHub using the next sequential index for that GitHub folder.
    Local file is not renamed; only the remote path uses the new index.
    """
    if not local_file_path:
        return False
    gh_last = get_last_index_from_github(github_folder)
    gh_next = gh_last + 1
    _, ext = os.path.splitext(local_file_path)
    remote_name = f"part-{gh_next:05d}-tid{ext}"
    github_path = f"{github_folder}/{remote_name}"
    return upload_to_github(local_file_path, github_path, commit_msg)

def upload_all_files_from_folder(dbfs_folder, github_folder, commit_msg, subdirs=None):
    """Recursively upload Spark output files, renaming locally and preserving partitions.
    - Only processes fresh Spark part files (skips already-renamed files containing '-tid').
    - If subdirs is provided, restricts uploads to those relative subdirectories.
    Maintains per-folder indexing in GitHub to avoid collisions.
    """
    root_local = dbfs_folder.replace("dbfs:", "/dbfs")

    # Build list of directories to traverse
    dirs_to_walk = []
    if subdirs:
        for rel in subdirs:
            abs_dir = os.path.join(root_local, rel).replace("/", os.sep)
            if os.path.isdir(abs_dir):
                dirs_to_walk.append(abs_dir)
    else:
        dirs_to_walk = [root_local]

    for start_dir in dirs_to_walk:
        for dirpath, _, filenames in os.walk(start_dir):
            # Only fresh Spark outputs; ignore already-renamed '-tid' files and control files
            part_files = sorted([
                fn for fn in filenames
                if fn.startswith("part-") and "-tid" not in fn
            ])
            if not part_files:
                continue

            rel_dir = os.path.relpath(dirpath, root_local).replace("\\", "/")
            gh_subfolder = f"{github_folder}/{rel_dir}" if rel_dir != "." else github_folder

            last_idx = get_last_index_from_github(gh_subfolder)
            next_idx = last_idx + 1

            for file in part_files:
                file_ext = os.path.splitext(file)[1]
                new_name = f"part-{next_idx:05d}-tid{file_ext}"

                old_path = os.path.join(dirpath, file)
                new_path = os.path.join(dirpath, new_name)

                if not os.path.exists(new_path):
                    os.rename(old_path, new_path)

                github_path = f"{gh_subfolder}/{new_name}"
                upload_to_github(new_path, github_path, commit_msg)

                next_idx += 1

# === Tower Data Generation ===
def generate_tower_locations_once():
    """Generate tower locations (only needed once) and store as Parquet."""
    regions = [
        "Gauteng", "KwaZulu-Natal", "Western Cape", "Eastern Cape", "Free State",
        "Mpumalanga", "Northern Cape", "Limpopo", "North West"
    ]
    regions_sql_array = ", ".join([f"'{r}'" for r in regions])

    tower_locations = spark.range(15000).select(
        col("id").cast("string").alias("tower_id"),
        (rand() * 7 + 22).alias("latitude"),
        (rand() * 9 + 16).alias("longitude")
    ).withColumn(
        "region_index", pmod(spark_hash(col("tower_id")), lit(len(regions))).cast("int")
    ).withColumn(
        "region", expr(f"element_at(array({regions_sql_array}), region_index + 1)")
    )
    tower_path = "dbfs:/mnt/dlstelkomnetworkprod/raw/tower_locations"
    # Single parquet file for tower locations
    _clean_dbfs_path(tower_path)
    tower_locations.coalesce(1).write.format("parquet").mode("overwrite").save(tower_path)
    upload_all_files_from_folder(tower_path, "data/tower_locations", "Initial tower locations dataset")
    return tower_locations

# === Timestamp Utilities ===
def random_timestamp_in_interval(start_time, end_time):
    """Generate random timestamp (as timestamp type) within a specific interval."""
    start_ts = int(start_time.timestamp())
    end_ts = int(end_time.timestamp())
    return expr(f"to_timestamp(from_unixtime({start_ts} + cast(rand() * {end_ts - start_ts} as bigint)))")

# === Main Data Generation ===
def _clean_dbfs_path(dbfs_path: str):
    local = dbfs_path.replace("dbfs:", "/dbfs")
    if os.path.exists(local):
        shutil.rmtree(local)


def generate_interval_data():
    """Generate data for the previous 4-hour interval and upload one file per dataset."""
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=4)

    # Load or generate tower locations
    try:
        tower_locations = spark.read.format("parquet").load("dbfs:/mnt/dlstelkomnetworkprod/raw/tower_locations")
        tower_locations = tower_locations.limit(15000)
    except:
        tower_locations = generate_tower_locations_once()

    tower_locations_df = tower_locations.withColumn("tower_index", monotonically_increasing_id())
    tower_count = tower_locations_df.count()

    # === Network Logs ===
    network_logs = spark.range(30000).select(
        (rand() * tower_count).cast("long").alias("tower_index")
    ).join(
        tower_locations_df, on="tower_index", how="left"
    ).select(
        col("tower_id"),
        (rand() * 100).alias("signal_strength"),
        (rand() * 10).alias("latency_ms"),
        random_timestamp_in_interval(start_time, end_time).alias("timestamp"),
        (rand() * 5 + 95).alias("uptime"),
        expr("CASE WHEN rand() < 0.94 THEN NULL WHEN rand() < 0.5 THEN 'E001' ELSE 'E002' END").alias("error_codes")
    ).withColumn(
        "signal_strength",
        when(col("signal_strength") < 20, col("signal_strength") * 0.8).otherwise(col("signal_strength"))
    ).withColumn(
        "year", year(col("timestamp"))
    ).withColumn(
        "month", month(col("timestamp"))
    ).withColumn(
        "day", dayofmonth(col("timestamp"))
    ).withColumn(
        "region_index", pmod(spark_hash(col("tower_id")), lit(9)).cast("int")
    ).withColumn(
        "region", expr("element_at(array('Gauteng','KwaZulu-Natal','Western Cape','Eastern Cape','Free State','Mpumalanga','Northern Cape','Limpopo','North West'), region_index + 1)")
    )
    network_path = "dbfs:/mnt/dlstelkomnetworkprod/raw/network_logs"
    tmp_nw = f"{network_path}/.tmp_interval_{int(end_time.timestamp())}"
    (network_logs
     .coalesce(1)
     .write.format("parquet").mode("overwrite").save(tmp_nw)
    )
    tmp_local = tmp_nw.replace("dbfs:", "/dbfs")
    root_local = network_path.replace("dbfs:", "/dbfs")
    os.makedirs(root_local, exist_ok=True)
    nw_local_file = ""
    if os.path.isdir(tmp_local):
        next_idx = get_last_index_from_local(root_local) + 1
        part_files = [f for f in os.listdir(tmp_local) if f.startswith("part-")]
        if part_files:
            src = os.path.join(tmp_local, part_files[0])
            nw_local_file = os.path.join(root_local, f"part-{next_idx:05d}-tid.parquet")
            os.replace(src, nw_local_file)
        shutil.rmtree(tmp_local, ignore_errors=True)

    # === Customer Usage ===
    customer_usage = spark.range(30000).select(
        col("id").cast("string").alias("customer_id"),
        (rand() * 1000).alias("data_usage_mb"),
        (rand() * 60).alias("call_duration_min"),
        random_timestamp_in_interval(start_time, end_time).alias("timestamp")
    ).withColumn(
        "data_usage_mb",
        when(col("data_usage_mb") > 800, col("data_usage_mb") * 1.2).otherwise(col("data_usage_mb"))
    ).withColumn(
        "data_usage", col("data_usage_mb")
    ).withColumn(
        "call_duration", col("call_duration_min")
    ).withColumn(
        "year", year(col("timestamp"))
    ).withColumn(
        "month", month(col("timestamp"))
    ).withColumn(
        "day", dayofmonth(col("timestamp"))
    )
    customer_path = "dbfs:/mnt/dlstelkomnetworkprod/raw/customer_usage"
    tmp_cu = f"{customer_path}/.tmp_interval_{int(end_time.timestamp())}"
    (customer_usage
     .coalesce(1)
     .write.format("parquet").mode("overwrite").save(tmp_cu)
    )
    tmp_local = tmp_cu.replace("dbfs:", "/dbfs")
    root_local = customer_path.replace("dbfs:", "/dbfs")
    os.makedirs(root_local, exist_ok=True)
    cu_local_file = ""
    if os.path.isdir(tmp_local):
        next_idx = get_last_index_from_local(root_local) + 1
        part_files = [f for f in os.listdir(tmp_local) if f.startswith("part-")]
        if part_files:
            src = os.path.join(tmp_local, part_files[0])
            cu_local_file = os.path.join(root_local, f"part-{next_idx:05d}-tid.parquet")
            os.replace(src, cu_local_file)
        shutil.rmtree(tmp_local, ignore_errors=True)

    # === Weather Data ===
    tower_dates = network_logs.select("tower_id", "timestamp").distinct()
    weather_data = tower_dates.withColumn(
        "temperature_c", (rand() * 15 + 20).cast("double")
    ).withColumn(
        "humidity_percent", (rand() * 40 + 40).cast("double")
    ).withColumn(
        "wind_speed_mps", (rand() * 10).cast("double")
    ).withColumn(
        "weather_condition", expr(
            "CASE WHEN rand() < 0.2 THEN 'Rain' "
            "WHEN rand() < 0.5 THEN 'Clouds' "
            "ELSE 'Clear' END"
        )
    ).withColumn(
        "visibility_km", (rand() * 10).cast("double")
    ).withColumn(
        "year", year(col("timestamp"))
    ).withColumn(
        "month", month(col("timestamp"))
    ).withColumn(
        "day", dayofmonth(col("timestamp"))
    ).withColumn(
        "region_index", pmod(spark_hash(col("tower_id")), lit(9)).cast("int")
    ).withColumn(
        "region", expr("element_at(array('Gauteng','KwaZulu-Natal','Western Cape','Eastern Cape','Free State','Mpumalanga','Northern Cape','Limpopo','North West'), region_index + 1)")
    )
    weather_path = "dbfs:/mnt/dlstelkomnetworkprod/raw/weather_data"
    tmp_w = f"{weather_path}/.tmp_interval_{int(end_time.timestamp())}"
    (weather_data
     .coalesce(1)
     .write.format("parquet").mode("overwrite").save(tmp_w)
    )
    tmp_local = tmp_w.replace("dbfs:", "/dbfs")
    root_local = weather_path.replace("dbfs:", "/dbfs")
    os.makedirs(root_local, exist_ok=True)
    w_local_file = ""
    if os.path.isdir(tmp_local):
        next_idx = get_last_index_from_local(root_local) + 1
        part_files = [f for f in os.listdir(tmp_local) if f.startswith("part-")]
        if part_files:
            src = os.path.join(tmp_local, part_files[0])
            w_local_file = os.path.join(root_local, f"part-{next_idx:05d}-tid.parquet")
            os.replace(src, w_local_file)
        shutil.rmtree(tmp_local, ignore_errors=True)

    # === Load Shedding Schedules ===
    load_shedding = spark.range(5000).select(
        expr("element_at(array('Gauteng','KwaZulu-Natal','Western Cape','Eastern Cape','Free State','Mpumalanga','Northern Cape','Limpopo','North West'), cast(rand() * 9 as int) + 1)").alias("region"),
        random_timestamp_in_interval(start_time, end_time).alias("start_time")
    ).withColumn(
        "end_time", expr("timestampadd(HOUR, cast(rand() * 3 + 1 as int), start_time)")
    ).withColumn(
        "year", year(col("start_time"))
    ).withColumn(
        "month", month(col("start_time"))
    ).withColumn(
        "day", dayofmonth(col("start_time"))
    )
    load_path = "dbfs:/mnt/dlstelkomnetworkprod/raw/load_shedding_schedules"
    tmp_ls = f"{load_path}/.tmp_interval_{int(end_time.timestamp())}"
    (load_shedding
     .coalesce(1)
     .write.format("parquet").mode("overwrite").save(tmp_ls)
    )
    tmp_local = tmp_ls.replace("dbfs:", "/dbfs")
    root_local = load_path.replace("dbfs:", "/dbfs")
    os.makedirs(root_local, exist_ok=True)
    ls_local_file = ""
    if os.path.isdir(tmp_local):
        next_idx = get_last_index_from_local(root_local) + 1
        part_files = [f for f in os.listdir(tmp_local) if f.startswith("part-")]
        if part_files:
            src = os.path.join(tmp_local, part_files[0])
            ls_local_file = os.path.join(root_local, f"part-{next_idx:05d}-tid.parquet")
            os.replace(src, ls_local_file)
        shutil.rmtree(tmp_local, ignore_errors=True)

    # === New Datasets (interval behavior, one file per interval) ===

    # Customer Feedback
    customer_feedback = spark.range(20000).select(
        expr(
            "element_at(array("
            "'Network is slow in my area',"
            "'Frequent dropped calls',"
            "'No coverage inside my building',"
            "'High latency during peak hours',"
            "'Billing issue with last invoice',"
            "'SIM card not working',"
            "'App keeps crashing',"
            "'Unexpected charges on my account',"
            "'Activation is delayed',"
            "'Roaming does not work'"
            "), cast(rand() * 10 as int) + 1)"
        ).alias("text"),
        expr("round(rand() * 2 - 1, 3)").alias("sentiment_score"),
        random_timestamp_in_interval(start_time, end_time).alias("timestamp")
    ).withColumn(
        "sentiment_label",
        expr("CASE WHEN sentiment_score < -0.2 THEN 'negative' WHEN sentiment_score > 0.2 THEN 'positive' ELSE 'neutral' END")
    ).withColumn("year", year(col("timestamp")))
    .withColumn("month", month(col("timestamp")))
    .withColumn("day", dayofmonth(col("timestamp")))
    cf_path = "dbfs:/mnt/dlstelkomnetworkprod/raw/customer_feedback"
    tmp_cf = f"{cf_path}/.tmp_interval_{int(end_time.timestamp())}"
    (customer_feedback
     .coalesce(1)
     .write.format("parquet").mode("overwrite").save(tmp_cf)
    )
    tmp_local = tmp_cf.replace("dbfs:", "/dbfs")
    root_local = cf_path.replace("dbfs:", "/dbfs")
    os.makedirs(root_local, exist_ok=True)
    cf_local_file = ""
    if os.path.isdir(tmp_local):
        next_idx = get_last_index_from_local(root_local) + 1
        part_files = [f for f in os.listdir(tmp_local) if f.startswith("part-")]
        if part_files:
            src = os.path.join(tmp_local, part_files[0])
            cf_local_file = os.path.join(root_local, f"part-{next_idx:05d}-tid.parquet")
            os.replace(src, cf_local_file)
        shutil.rmtree(tmp_local, ignore_errors=True)

    # Tower Imagery
    tower_imagery = tower_locations_df.select(
        col("tower_id"),
        random_timestamp_in_interval(start_time, end_time).alias("timestamp")
    ).withColumn(
        "image_path",
        expr("concat('dbfs:/mnt/dlstelkomnetworkprod/assets/tower_images/', tower_id, '/', cast(unix_timestamp(timestamp) as string), '.jpg')")
    ).withColumn(
        "condition_label",
        expr("element_at(array('OK','Minor','Critical'), cast(rand() * 3 as int) + 1)")
    ).withColumn("year", year(col("timestamp")))
    .withColumn("month", month(col("timestamp")))
    .withColumn("day", dayofmonth(col("timestamp")))
    ti_path = "dbfs:/mnt/dlstelkomnetworkprod/raw/tower_imagery"
    tmp_ti = f"{ti_path}/.tmp_interval_{int(end_time.timestamp())}"
    (tower_imagery
     .coalesce(1)
     .write.format("parquet").mode("overwrite").save(tmp_ti)
    )
    tmp_local = tmp_ti.replace("dbfs:", "/dbfs")
    root_local = ti_path.replace("dbfs:", "/dbfs")
    os.makedirs(root_local, exist_ok=True)
    ti_local_file = ""
    if os.path.isdir(tmp_local):
        next_idx = get_last_index_from_local(root_local) + 1
        part_files = [f for f in os.listdir(tmp_local) if f.startswith("part-")]
        if part_files:
            src = os.path.join(tmp_local, part_files[0])
            ti_local_file = os.path.join(root_local, f"part-{next_idx:05d}-tid.parquet")
            os.replace(src, ti_local_file)
        shutil.rmtree(tmp_local, ignore_errors=True)

    # Voice Transcriptions
    voice_transcriptions = spark.range(6000).select(
        col("id").cast("string").alias("technician_id"),
        expr(
            "element_at(array(" 
            "'Start site inspection'," 
            "'Check signal levels'," 
            "'Replace faulty antenna'," 
            "'Confirm power status'," 
            "'Log maintenance complete'," 
            "'Escalate to network ops'," 
            "'Verify backhaul link'," 
            "'Capture tower imagery'," 
            "'Run diagnostic test'," 
            "'Close the ticket'" 
            "), cast(rand() * 10 as int) + 1)"
        ).alias("transcription"),
        random_timestamp_in_interval(start_time, end_time).alias("timestamp")
    ).withColumn("year", year(col("timestamp")))
    .withColumn("month", month(col("timestamp")))
    .withColumn("day", dayofmonth(col("timestamp")))
    vt_path = "dbfs:/mnt/dlstelkomnetworkprod/raw/voice_transcriptions"
    tmp_vt = f"{vt_path}/.tmp_interval_{int(end_time.timestamp())}"
    (voice_transcriptions
     .coalesce(1)
     .write.format("parquet").mode("overwrite").save(tmp_vt)
    )
    tmp_local = tmp_vt.replace("dbfs:", "/dbfs")
    root_local = vt_path.replace("dbfs:", "/dbfs")
    os.makedirs(root_local, exist_ok=True)
    vt_local_file = ""
    if os.path.isdir(tmp_local):
        next_idx = get_last_index_from_local(root_local) + 1
        part_files = [f for f in os.listdir(tmp_local) if f.startswith("part-")]
        if part_files:
            src = os.path.join(tmp_local, part_files[0])
            vt_local_file = os.path.join(root_local, f"part-{next_idx:05d}-tid.parquet")
            os.replace(src, vt_local_file)
        shutil.rmtree(tmp_local, ignore_errors=True)

    # Tower Connectivity (each tower connects to two neighbors)
    neighbor_map = tower_locations_df.select(
        col("tower_id").alias("dst_tower_id"),
        expr("row_number() over (order by tower_id) - 1").alias("neighbor_index")
    )
    src_with_neighbors = tower_locations_df.select(
        col("tower_id").alias("src_tower_id"),
        expr("row_number() over (order by tower_id) - 1").alias("tower_index")
    )
    total_towers = neighbor_map.count()
    edges_idx = src_with_neighbors.select(
        col("src_tower_id"), expr(f"(tower_index + 1) % {total_towers}").alias("neighbor_index")
    ).union(
        src_with_neighbors.select(
            col("src_tower_id"), expr(f"(tower_index + 2) % {total_towers}").alias("neighbor_index")
        )
    )
    tower_connectivity = edges_idx.join(neighbor_map, on="neighbor_index", how="left").select(
        col("src_tower_id"),
        col("dst_tower_id"),
        (rand() * 100).alias("signal_quality"),
        random_timestamp_in_interval(start_time, end_time).alias("timestamp")
    ).withColumn("year", year(col("timestamp")))
    .withColumn("month", month(col("timestamp")))
    .withColumn("day", dayofmonth(col("timestamp")))
    tc_path = "dbfs:/mnt/dlstelkomnetworkprod/raw/tower_connectivity"
    tmp_tc = f"{tc_path}/.tmp_interval_{int(end_time.timestamp())}"
    (tower_connectivity
     .coalesce(1)
     .write.format("parquet").mode("overwrite").save(tmp_tc)
    )
    tmp_local = tmp_tc.replace("dbfs:", "/dbfs")
    root_local = tc_path.replace("dbfs:", "/dbfs")
    os.makedirs(root_local, exist_ok=True)
    tc_local_file = ""
    if os.path.isdir(tmp_local):
        next_idx = get_last_index_from_local(root_local) + 1
        part_files = [f for f in os.listdir(tmp_local) if f.startswith("part-")]
        if part_files:
            src = os.path.join(tmp_local, part_files[0])
            tc_local_file = os.path.join(root_local, f"part-{next_idx:05d}-tid.parquet")
            os.replace(src, tc_local_file)
        shutil.rmtree(tmp_local, ignore_errors=True)

    # Tower Capacity
    tower_capacity = tower_locations_df.select(
        col("tower_id"),
        (rand() * 900 + 100).alias("capacity_mbps"),
        (rand() * 80 + 10).alias("utilization_percent"),
        random_timestamp_in_interval(start_time, end_time).alias("timestamp")
    ).withColumn("year", year(col("timestamp")))
    .withColumn("month", month(col("timestamp")))
    .withColumn("day", dayofmonth(col("timestamp")))
    tcap_path = "dbfs:/mnt/dlstelkomnetworkprod/raw/tower_capacity"
    tmp_tcap = f"{tcap_path}/.tmp_interval_{int(end_time.timestamp())}"
    (tower_capacity
     .coalesce(1)
     .write.format("parquet").mode("overwrite").save(tmp_tcap)
    )
    tmp_local = tmp_tcap.replace("dbfs:", "/dbfs")
    root_local = tcap_path.replace("dbfs:", "/dbfs")
    os.makedirs(root_local, exist_ok=True)
    tcap_local_file = ""
    if os.path.isdir(tmp_local):
        next_idx = get_last_index_from_local(root_local) + 1
        part_files = [f for f in os.listdir(tmp_local) if f.startswith("part-")]
        if part_files:
            src = os.path.join(tmp_local, part_files[0])
            tcap_local_file = os.path.join(root_local, f"part-{next_idx:05d}-tid.parquet")
            os.replace(src, tcap_local_file)
        shutil.rmtree(tmp_local, ignore_errors=True)

    # Maintenance Crew
    maintenance_crew = spark.range(2000).select(
        col("id").cast("string").alias("crew_id"),
        expr("element_at(array('Gauteng','KwaZulu-Natal','Western Cape','Eastern Cape','Free State','Mpumalanga','Northern Cape','Limpopo','North West'), cast(rand() * 9 as int) + 1)").alias("region"),
        (rand() * 7 + 22).alias("latitude"),
        (rand() * 9 + 16).alias("longitude"),
        expr("rand() < 0.7").alias("available"),
        random_timestamp_in_interval(start_time, end_time).alias("shift_start")
    ).withColumn(
        "shift_end", expr("timestampadd(HOUR, 8, shift_start)")
    ).withColumn("year", year(col("shift_start")))
    .withColumn("month", month(col("shift_start")))
    .withColumn("day", dayofmonth(col("shift_start")))
    mc_path = "dbfs:/mnt/dlstelkomnetworkprod/raw/maintenance_crew"
    tmp_mc = f"{mc_path}/.tmp_interval_{int(end_time.timestamp())}"
    (maintenance_crew
     .coalesce(1)
     .write.format("parquet").mode("overwrite").save(tmp_mc)
    )
    tmp_local = tmp_mc.replace("dbfs:", "/dbfs")
    root_local = mc_path.replace("dbfs:", "/dbfs")
    os.makedirs(root_local, exist_ok=True)
    mc_local_file = ""
    if os.path.isdir(tmp_local):
        next_idx = get_last_index_from_local(root_local) + 1
        part_files = [f for f in os.listdir(tmp_local) if f.startswith("part-")]
        if part_files:
            src = os.path.join(tmp_local, part_files[0])
            mc_local_file = os.path.join(root_local, f"part-{next_idx:05d}-tid.parquet")
            os.replace(src, mc_local_file)
        shutil.rmtree(tmp_local, ignore_errors=True)

    # === Upload to GitHub ===
    commit_msg = f"Data update for interval {start_time.strftime('%Y-%m-%d %H:%M')} - {end_time.strftime('%H:%M')}"
    upload_file_with_github_index(nw_local_file, "data/network_logs", commit_msg)
    upload_file_with_github_index(cu_local_file, "data/customer_usage", commit_msg)
    upload_file_with_github_index(w_local_file, "data/weather_data", commit_msg)
    upload_file_with_github_index(ls_local_file, "data/load_shedding_schedules", commit_msg)

    # New dataset uploads
    upload_file_with_github_index(cf_local_file, "data/customer_feedback", commit_msg)
    upload_file_with_github_index(ti_local_file, "data/tower_imagery", commit_msg)
    upload_file_with_github_index(vt_local_file, "data/voice_transcriptions", commit_msg)
    upload_file_with_github_index(tc_local_file, "data/tower_connectivity", commit_msg)
    upload_file_with_github_index(tcap_local_file, "data/tower_capacity", commit_msg)
    upload_file_with_github_index(mc_local_file, "data/maintenance_crew", commit_msg)

    return True

# === Main Execution ===
if __name__ == "__main__":
    generate_interval_data()

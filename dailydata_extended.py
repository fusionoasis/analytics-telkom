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
from uuid import uuid4

# === GitHub Configuration ===
GITHUB_REPO = "fusionoasis/analytics-data"
GITHUB_API_URL = f"https://api.github.com/repos/{GITHUB_REPO}/contents"

spark = SparkSession.builder.getOrCreate()

# Provide a dbutils shim for non-Databricks environments and avoid NameError in linters
try:  # noqa: SIM105
    dbutils  # type: ignore[name-defined]
except NameError:  # pragma: no cover - only for local dev
    try:
        from pyspark.dbutils import DBUtils  # type: ignore
        dbutils = DBUtils(spark)  # type: ignore
    except Exception:  # fallback stub
        class _DBFS:
            def mkdirs(self, *args, **kwargs):
                return None
            def ls(self, *args, **kwargs):
                return []
            def rm(self, *args, **kwargs):
                return None
            def mv(self, *args, **kwargs):
                return None
            def cp(self, *args, **kwargs):
                return None
        class _Secrets:
            def get(self, *args, **kwargs):
                raise RuntimeError("dbutils.secrets not available")
        class _DBUtils:
            fs = _DBFS()
            secrets = _Secrets()
        dbutils = _DBUtils()  # type: ignore

def _get_github_token() -> str:
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GITHUB_PAT")
    if token:
        return token
    try:
        return dbutils.secrets.get(scope="databricksazure", key="github-pat-token")  # type: ignore[attr-defined]
    except Exception:
        return ""

GITHUB_TOKEN = _get_github_token()

# === Storage base (ABFSS) ===
RAW_BASE = "abfss://raw@dlstelkomnetworkprod.dfs.core.windows.net"

# === Path/FS helpers (support abfss/dbfs) ===
def _is_abfss(path: str) -> bool:
    return isinstance(path, str) and path.startswith("abfss:")

def _is_dbfs(path: str) -> bool:
    return isinstance(path, str) and path.startswith("dbfs:")

def _ensure_dir(path: str):
    try:
        dbutils.fs.mkdirs(path)
    except Exception:
        pass

def _list_parts(path: str):
    try:
        return [fi for fi in dbutils.fs.ls(path) if fi.name.startswith("part-")]
    except Exception:
        return []

def _rm_path(path: str):
    try:
        if _is_abfss(path) or _is_dbfs(path):
            dbutils.fs.rm(path, True)
        else:
            local = path.replace("dbfs:", "/dbfs")
            if os.path.exists(local):
                shutil.rmtree(local, ignore_errors=True)
    except Exception:
        pass

def _get_last_index_from_remote(folder_path: str) -> int:
    try:
        files = dbutils.fs.ls(folder_path)
    except Exception:
        return -1
    max_idx = -1
    for fi in files:
        name = fi.name.rstrip('/')
        if not name.startswith("part-"):
            continue
        try:
            core = name.split("-")[1]
            idx = int(core)
            max_idx = max(max_idx, idx)
        except Exception:
            continue
    return max_idx

def _move_single_part_from_tmp(tmp_dir: str, dest_dir: str) -> str:
    """Move the single part file from tmp_dir into dest_dir with next sequential index.
    Returns the destination file path (abfss/dbfs) or empty string if none.
    """
    _ensure_dir(dest_dir)
    parts = _list_parts(tmp_dir)
    if not parts:
        _rm_path(tmp_dir)
        return ""
    next_idx = _get_last_index_from_remote(dest_dir) + 1
    src = parts[0].path
    ext = os.path.splitext(parts[0].name)[1] or ".parquet"
    dest = f"{dest_dir}/part-{next_idx:05d}-tid{ext}"
    dbutils.fs.mv(src, dest, True)
    _rm_path(tmp_dir)
    return dest

def _to_local_for_upload(path: str) -> str:
    """Ensure file is available on local driver filesystem for upload (returns /dbfs/... path)."""
    if _is_dbfs(path):
        return path.replace("dbfs:", "/dbfs")
    if _is_abfss(path):
        tmp_dbfs = f"dbfs:/tmp/github_uploads/{uuid4().hex}-{os.path.basename(path)}"
        _ensure_dir("dbfs:/tmp/github_uploads")
        dbutils.fs.cp(path, tmp_dbfs, True)
        return tmp_dbfs.replace("dbfs:", "/dbfs")
    # Fallback: assume it's already local
    return path

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
    local_path = _to_local_for_upload(file_path)
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
    """Deprecated: retained for compatibility. Not used with abfss."""
    try:
        names = os.listdir(local_folder)
    except Exception:
        return -1
    max_idx = -1
    for name in names:
        if not name.startswith("part-"):
            continue
        try:
            core = name.split("-")[1]
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
    """Legacy helper not used in abfss flow. Kept for compatibility."""
    return

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
    tower_path = f"{RAW_BASE}/tower_locations"
    _clean_dbfs_path(tower_path)
    tmp_tower = f"{tower_path}/.tmp_init_{int(datetime.now().timestamp())}"
    tower_locations.coalesce(1).write.format("parquet").mode("overwrite").save(tmp_tower)
    dest_file = _move_single_part_from_tmp(tmp_tower, tower_path)
    if dest_file:
        upload_file_with_github_index(dest_file, "data/tower_locations", "Initial tower locations dataset")
    return tower_locations

# === Timestamp Utilities ===
def random_timestamp_in_interval(start_time, end_time):
    """Generate random timestamp (as timestamp type) within a specific interval."""
    start_ts = int(start_time.timestamp())
    end_ts = int(end_time.timestamp())
    return expr(f"to_timestamp(from_unixtime({start_ts} + cast(rand() * {end_ts - start_ts} as bigint)))")

# === Main Data Generation ===
def _clean_dbfs_path(dbfs_path: str):
    _rm_path(dbfs_path)


def generate_interval_data():
    """Generate data for the previous 4-hour interval and upload one file per dataset."""
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=4)

    # Load or generate tower locations
    try:
        tower_locations = spark.read.format("parquet").load(f"{RAW_BASE}/tower_locations")
        tower_locations = tower_locations.limit(15000)
    except:
        tower_locations = generate_tower_locations_once()

    tower_locations_df = tower_locations.withColumn("tower_index", monotonically_increasing_id())
    tower_count = tower_locations_df.count()

    # Deterministic indexing helpers (do not alter tower_locations_df usage)
    towers_index_lookup = (
        tower_locations.select("tower_id")
        .withColumn("tower_index", expr("row_number() over (order by tower_id) - 1"))
        .select("tower_index", "tower_id")
    )
    total_towers = towers_index_lookup.count()
    towers_by_region = (
        tower_locations.select("region", "tower_id")
        .withColumn("region_rank", expr("row_number() over (partition by region order by tower_id)"))
    )
    region_counts = tower_locations.groupBy("region").count().withColumnRenamed("count", "region_tower_count")

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
    network_path = f"{RAW_BASE}/network_logs"
    tmp_nw = f"{network_path}/.tmp_interval_{int(end_time.timestamp())}"
    (network_logs
     .coalesce(1)
     .write.format("parquet").mode("overwrite").save(tmp_nw)
    )
    nw_remote_file = _move_single_part_from_tmp(tmp_nw, network_path)

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
    customer_path = f"{RAW_BASE}/customer_usage"
    tmp_cu = f"{customer_path}/.tmp_interval_{int(end_time.timestamp())}"
    # Enrich with tower_id deterministically based on customer_id
    customer_usage = (
        customer_usage
        .withColumn("tower_index", pmod(spark_hash(col("customer_id")), lit(total_towers)).cast("int"))
        .join(towers_index_lookup, on="tower_index", how="left")
        .drop("tower_index")
    )
    (customer_usage
     .coalesce(1)
     .write.format("parquet").mode("overwrite").save(tmp_cu)
    )
    cu_remote_file = _move_single_part_from_tmp(tmp_cu, customer_path)

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
    weather_path = f"{RAW_BASE}/weather_data"
    tmp_w = f"{weather_path}/.tmp_interval_{int(end_time.timestamp())}"
    (weather_data
     .coalesce(1)
     .write.format("parquet").mode("overwrite").save(tmp_w)
    )
    w_remote_file = _move_single_part_from_tmp(tmp_w, weather_path)

    # Customer Feedback
    customer_feedback = (
        spark.range(20000).select(
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
        )
        .withColumn(
            "sentiment_label",
            expr("CASE WHEN sentiment_score < -0.2 THEN 'negative' WHEN sentiment_score > 0.2 THEN 'positive' ELSE 'neutral' END")
        )
        .withColumn("year", year(col("timestamp")))
        .withColumn("month", month(col("timestamp")))
        .withColumn("day", dayofmonth(col("timestamp")))
    )
    
    cf_path = f"{RAW_BASE}/customer_feedback"
    tmp_cf = f"{cf_path}/.tmp_interval_{int(end_time.timestamp())}"
    # Enrich with tower_id deterministically based on feedback text and timestamp
    customer_feedback = (
        customer_feedback
        .withColumn("tower_index", pmod(spark_hash(expr("concat(text, cast(timestamp as string))")), lit(total_towers)).cast("int"))
        .join(towers_index_lookup, on="tower_index", how="left")
        .drop("tower_index")
    )
    (customer_feedback
     .coalesce(1)
     .write.format("parquet").mode("overwrite").save(tmp_cf)
    )
    cf_remote_file = _move_single_part_from_tmp(tmp_cf, cf_path)

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
    ).withColumn("year", year(col("timestamp"))) \
    .withColumn("month", month(col("timestamp"))) \
    .withColumn("day", dayofmonth(col("timestamp")))
    
    vt_path = f"{RAW_BASE}/voice_transcriptions"
    tmp_vt = f"{vt_path}/.tmp_interval_{int(end_time.timestamp())}"
    # Enrich with tower_id deterministically based on technician_id
    voice_transcriptions = (
        voice_transcriptions
        .withColumn("tower_index", pmod(spark_hash(col("technician_id")), lit(total_towers)).cast("int"))
        .join(towers_index_lookup, on="tower_index", how="left")
        .drop("tower_index")
    )
    (voice_transcriptions
     .coalesce(1)
     .write.format("parquet").mode("overwrite").save(tmp_vt)
    )
    vt_remote_file = _move_single_part_from_tmp(tmp_vt, vt_path)

    # === Load Shedding Schedules (Interval) ===
    load_shedding = (
        spark.range(2000)
        .select(
            expr(
                "element_at(array('Gauteng','KwaZulu-Natal','Western Cape','Eastern Cape','Free State','Mpumalanga','Northern Cape','Limpopo','North West'), cast(rand() * 9 as int) + 1)"
            ).alias("region"),
            random_timestamp_in_interval(start_time, end_time).alias("start_time"),
        )
        .withColumn("end_time", expr("timestampadd(HOUR, cast(rand() * 3 + 1 as int), start_time)"))
        .withColumn("year", year(col("start_time")))
        .withColumn("month", month(col("start_time")))
        .withColumn("day", dayofmonth(col("start_time")))
        .join(region_counts, on="region", how="left")
        .withColumn(
            "region_rank",
            (pmod(spark_hash(expr("concat(region, cast(start_time as string))")), col("region_tower_count")) + 1).cast("int")
        )
        .join(towers_by_region, on=["region", "region_rank"], how="left")
        .drop("region_tower_count", "region_rank")
    )
    ls_path = f"{RAW_BASE}/load_shedding_schedules"
    tmp_ls = f"{ls_path}/.tmp_interval_{int(end_time.timestamp())}"
    (load_shedding
     .coalesce(1)
     .write.format("parquet").mode("overwrite").save(tmp_ls)
    )
    ls_remote_file = _move_single_part_from_tmp(tmp_ls, ls_path)

    # === Maintenance Crew (Interval) ===
    maintenance_crew = (
        spark.range(800)
        .select(
            col("id").cast("string").alias("crew_id"),
            expr(
                "element_at(array('Gauteng','KwaZulu-Natal','Western Cape','Eastern Cape','Free State','Mpumalanga','Northern Cape','Limpopo','North West'), cast(rand() * 9 as int) + 1)"
            ).alias("region"),
            (rand() * 7 + 22).alias("latitude"),
            (rand() * 9 + 16).alias("longitude"),
            expr("rand() < 0.7").alias("available"),
            random_timestamp_in_interval(start_time, end_time).alias("shift_start"),
        )
        .withColumn("shift_end", expr("timestampadd(HOUR, 8, shift_start)"))
        .withColumn("year", year(col("shift_start")))
        .withColumn("month", month(col("shift_start")))
        .withColumn("day", dayofmonth(col("shift_start")))
        .join(region_counts, on="region", how="left")
        .withColumn(
            "region_rank",
            (pmod(spark_hash(expr("concat(crew_id, cast(shift_start as string))")), col("region_tower_count")) + 1).cast("int")
        )
        .join(towers_by_region, on=["region", "region_rank"], how="left")
        .drop("region_tower_count", "region_rank")
    )
    mc_path = f"{RAW_BASE}/maintenance_crew"
    tmp_mc = f"{mc_path}/.tmp_interval_{int(end_time.timestamp())}"
    (maintenance_crew
     .coalesce(1)
     .write.format("parquet").mode("overwrite").save(tmp_mc)
    )
    mc_remote_file = _move_single_part_from_tmp(tmp_mc, mc_path)

    # === Upload to GitHub ===
    commit_msg = f"Data update for interval {start_time.strftime('%Y-%m-%d %H:%M')} - {end_time.strftime('%H:%M')}"
    upload_file_with_github_index(nw_remote_file, "data/network_logs", commit_msg)
    upload_file_with_github_index(cu_remote_file, "data/customer_usage", commit_msg)
    upload_file_with_github_index(w_remote_file, "data/weather_data", commit_msg)
    upload_file_with_github_index(cf_remote_file, "data/customer_feedback", commit_msg)
    upload_file_with_github_index(vt_remote_file, "data/voice_transcriptions", commit_msg)
    upload_file_with_github_index(ls_remote_file, "data/load_shedding_schedules", commit_msg)
    upload_file_with_github_index(mc_remote_file, "data/maintenance_crew", commit_msg)

    return True

# === Main Execution ===
if __name__ == "__main__":
    generate_interval_data()

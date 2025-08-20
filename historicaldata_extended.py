from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    rand, col, expr, when, year, month, dayofmonth, to_date, lit,
    hash as spark_hash, pmod
)
import requests
import base64
import os
import shutil
from datetime import datetime

# GitHub configuration (expects to run in Databricks with the secret configured)
GITHUB_TOKEN = dbutils.secrets.get(scope="databricksazure", key="github-pat-token")
GITHUB_REPO = "fusionoasis/analytics-telkom"
GITHUB_API_URL = f"https://api.github.com/repos/{GITHUB_REPO}/contents"

spark = SparkSession.builder.getOrCreate()


def get_last_index_from_github(github_folder):
    """Check the highest part number in a GitHub folder."""
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
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
        except Exception:
            continue
    return max(indices) if indices else -1


def upload_to_github(file_path, github_path, commit_message):
    """Upload or update a single file to GitHub repository via Contents API.
    If the file exists, include its SHA to update in place. Uses the same filename as local.
    """
    local_path = file_path.replace("dbfs:", "/dbfs")
    with open(local_path, "rb") as f:
        content = f.read()

    content_encoded = base64.b64encode(content).decode()

    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }

    # Try to get existing file SHA
    sha = None
    get_resp = requests.get(f"{GITHUB_API_URL}/{github_path}", headers=headers)
    if get_resp.status_code == 200:
        try:
            sha = get_resp.json().get("sha")
        except Exception:
            sha = None

    payload = {
        "message": commit_message,
        "content": content_encoded,
    }
    if sha:
        payload["sha"] = sha

    response = requests.put(f"{GITHUB_API_URL}/{github_path}", json=payload, headers=headers)
    return response.status_code in [200, 201]


def random_timestamp_expr():
    """Generate random timestamp between 2025-07-01 and 2025-08-19."""
    return expr(
        """
        timestampadd(
            SECOND,
            cast(rand() * 86400 as int),
            date_add(to_date('2025-07-01'), cast(rand() * 50 as int))
        )
        """
    )


def generate_timestamp_data():
    current_time = datetime.now()
    commit_msg = f"Data update {current_time.strftime('%Y%m%d_%H%M%S')}"

    regions = [
        "Gauteng",
        "KwaZulu-Natal",
        "Western Cape",
        "Eastern Cape",
        "Free State",
        "Mpumalanga",
        "Northern Cape",
        "Limpopo",
        "North West",
    ]
    regions_sql_array = ", ".join([f"'{r}'" for r in regions])

    # ---------- Tower Locations ----------
    tower_locations = (
        spark.range(15000)
        .select(
            col("id").cast("string").alias("tower_id"),
            (rand() * 7 + 22).alias("latitude"),
            (rand() * 9 + 16).alias("longitude"),
        )
        .withColumn(
            "region_index", pmod(spark_hash(col("tower_id")), lit(len(regions))).cast("int")
        )
        .withColumn("region", expr(f"element_at(array({regions_sql_array}), region_index + 1)"))
    )
    tower_parquet_path = "dbfs:/mnt/dlstelkomnetworkprod/raw/tower_locations"
    (
        tower_locations.coalesce(1)
        .write.format("parquet").mode("overwrite").option("header", "true").save(tower_parquet_path)
    )

    tower_locations_df = tower_locations.withColumn(
        "tower_index", expr("row_number() over (order by tower_id) - 1")
    )

    # Utility
    def _clean_dbfs_path(dbfs_path: str):
        local = dbfs_path.replace("dbfs:", "/dbfs")
        if os.path.exists(local):
            shutil.rmtree(local)

    def _rename_parts_in_folder(dbfs_folder: str):
        """Rename all Spark part files in a folder to a deterministic pattern:
        part-xxxxx-tid.parquet where xxxxx is 0..N-1. This operates only on
        files directly under the given folder (no recursion).
        """
        root_local = dbfs_folder.replace("dbfs:", "/dbfs")
        if not os.path.isdir(root_local):
            return
        # Collect only files that start with Spark's part prefix
        part_files = [fn for fn in os.listdir(root_local) if fn.startswith("part-") and os.path.isfile(os.path.join(root_local, fn))]
        if not part_files:
            return
        part_files.sort()  # stable rename order
        for idx, fname in enumerate(part_files):
            target = f"part-{idx:05d}-tid.parquet"
            src = os.path.join(root_local, fname)
            dst = os.path.join(root_local, target)
            # Skip if already in desired name
            if os.path.basename(src) == target:
                continue
            # Overwrite safely if a stale target exists
            if os.path.exists(dst):
                os.remove(dst)
            os.replace(src, dst)

    # ---------- Network Logs ----------
    network_logs = (
        spark.range(6000000)
        .select((rand() * tower_locations_df.count()).cast("int").alias("tower_index"))
        .join(tower_locations_df, on="tower_index")
        .select(
            col("tower_id"),
            (rand() * 100).alias("signal_strength"),
            (rand() * 10).alias("latency_ms"),
            random_timestamp_expr().alias("timestamp"),
            (rand() * 5 + 95).alias("uptime"),
            expr(
                "CASE WHEN rand() < 0.94 THEN NULL WHEN rand() < 0.5 THEN 'E001' ELSE 'E002' END"
            ).alias("error_codes"),
        )
        .withColumn(
            "signal_strength",
            when(col("signal_strength") < 20, col("signal_strength") * 0.8).otherwise(col("signal_strength")),
        )
        .withColumn("year", year(col("timestamp")))
        .withColumn("month", month(col("timestamp")))
        .withColumn("day", dayofmonth(col("timestamp")))
        .withColumn("region_index", pmod(spark_hash(col("tower_id")), lit(len(regions))).cast("int"))
        .withColumn("region", expr(f"element_at(array({regions_sql_array}), region_index + 1)"))
        .filter(
            (col("year") == 2025)
            & (col("timestamp") >= "2025-07-01 00:00:00")
            & (col("timestamp") <= "2025-08-19 23:59:59")
        )
    )
    network_parquet_path = "dbfs:/mnt/dlstelkomnetworkprod/raw/network_logs"
    _clean_dbfs_path(network_parquet_path)
    distinct_days_nw = [
        (r["year"], r["month"], r["day"]) for r in network_logs.select("year", "month", "day").distinct().collect()
    ]
    for y, m, d in distinct_days_nw:
        tmp_day_path = f"{network_parquet_path}/.tmp_day_{y}_{m}_{d}"
        (
            network_logs.filter((col("year") == y) & (col("month") == m) & (col("day") == d))
            .coalesce(1)
            .write.format("parquet").mode("overwrite").save(tmp_day_path)
        )
        tmp_local = tmp_day_path.replace("dbfs:", "/dbfs")
        root_local = network_parquet_path.replace("dbfs:", "/dbfs")
        if os.path.isdir(tmp_local):
            for fname in os.listdir(tmp_local):
                if fname.startswith("part-"):
                    os.replace(os.path.join(tmp_local, fname), os.path.join(root_local, fname))
            shutil.rmtree(tmp_local, ignore_errors=True)
    # Normalize filenames to part-xxxxx-tid.parquet (0..N-1)
    _rename_parts_in_folder(network_parquet_path)

    # ---------- Weather Data ----------
    tower_dates = network_logs.select("tower_id", "timestamp").distinct()
    weather_data = (
        tower_dates.withColumn("temperature_c", (rand() * 15 + 20).cast("double"))
        .withColumn("humidity_percent", (rand() * 40 + 40).cast("double"))
        .withColumn("wind_speed_mps", (rand() * 10).cast("double"))
        .withColumn(
            "weather_condition",
            expr("CASE WHEN rand() < 0.2 THEN 'Rain' WHEN rand() < 0.5 THEN 'Clouds' ELSE 'Clear' END"),
        )
        .withColumn("visibility_km", (rand() * 10).cast("double"))
        .withColumn("year", year(col("timestamp")))
        .withColumn("month", month(col("timestamp")))
        .withColumn("day", dayofmonth(col("timestamp")))
        .withColumn("region_index", pmod(spark_hash(col("tower_id")), lit(len(regions))).cast("int"))
        .withColumn("region", expr(f"element_at(array({regions_sql_array}), region_index + 1)"))
    )
    weather_parquet_path = "dbfs:/mnt/dlstelkomnetworkprod/raw/weather_data"
    _clean_dbfs_path(weather_parquet_path)
    distinct_days_w = [
        (r["year"], r["month"], r["day"]) for r in weather_data.select("year", "month", "day").distinct().collect()
    ]
    for y, m, d in distinct_days_w:
        tmp_day_path = f"{weather_parquet_path}/.tmp_day_{y}_{m}_{d}"
        (
            weather_data.filter((col("year") == y) & (col("month") == m) & (col("day") == d))
            .coalesce(1)
            .write.format("parquet").option("header", "true").mode("overwrite").save(tmp_day_path)
        )
        tmp_local = tmp_day_path.replace("dbfs:", "/dbfs")
        root_local = weather_parquet_path.replace("dbfs:", "/dbfs")
        if os.path.isdir(tmp_local):
            for fname in os.listdir(tmp_local):
                if fname.startswith("part-"):
                    os.replace(os.path.join(tmp_local, fname), os.path.join(root_local, fname))
            shutil.rmtree(tmp_local, ignore_errors=True)
    _rename_parts_in_folder(weather_parquet_path)

    # ---------- Customer Usage ----------
    customer_usage = (
        spark.range(6000000)
        .select(
            col("id").cast("string").alias("customer_id"),
            (rand() * 1000).alias("data_usage_mb"),
            (rand() * 60).alias("call_duration_min"),
            random_timestamp_expr().alias("timestamp"),
        )
        .withColumn(
            "data_usage_mb",
            when(col("data_usage_mb") > 800, col("data_usage_mb") * 1.2).otherwise(col("data_usage_mb")),
        )
        .filter(
            (year(col("timestamp")) == 2025)
            & (col("timestamp") >= "2025-07-01 00:00:00")
            & (col("timestamp") <= "2025-08-19 23:59:59")
        )
        .withColumn("data_usage", col("data_usage_mb"))
        .withColumn("call_duration", col("call_duration_min"))
        .withColumn("year", year(col("timestamp")))
        .withColumn("month", month(col("timestamp")))
        .withColumn("day", dayofmonth(col("timestamp")))
    )
    customer_parquet_path = "dbfs:/mnt/dlstelkomnetworkprod/raw/customer_usage"
    _clean_dbfs_path(customer_parquet_path)
    distinct_days_cu = [
        (r["year"], r["month"], r["day"]) for r in customer_usage.select("year", "month", "day").distinct().collect()
    ]
    for y, m, d in distinct_days_cu:
        tmp_day_path = f"{customer_parquet_path}/.tmp_day_{y}_{m}_{d}"
        (
            customer_usage.filter((col("year") == y) & (col("month") == m) & (col("day") == d))
            .coalesce(1)
            .write.format("parquet").mode("overwrite").save(tmp_day_path)
        )
        tmp_local = tmp_day_path.replace("dbfs:", "/dbfs")
        root_local = customer_parquet_path.replace("dbfs:", "/dbfs")
        if os.path.isdir(tmp_local):
            for fname in os.listdir(tmp_local):
                if fname.startswith("part-"):
                    os.replace(os.path.join(tmp_local, fname), os.path.join(root_local, fname))
            shutil.rmtree(tmp_local, ignore_errors=True)
    _rename_parts_in_folder(customer_parquet_path)

    # ---------- Load Shedding Schedules ----------
    load_shedding = (
        spark.range(20000)
        .select(
            expr(
                f"element_at(array({regions_sql_array}), cast(rand() * {len(regions)} as int) + 1)"
            ).alias("region"),
            random_timestamp_expr().alias("start_time"),
        )
        .withColumn("end_time", expr("timestampadd(HOUR, cast(rand() * 3 + 1 as int), start_time)"))
        .withColumn("year", year(col("start_time")))
        .withColumn("month", month(col("start_time")))
        .withColumn("day", dayofmonth(col("start_time")))
    )
    load_shedding_parquet_path = "dbfs:/mnt/dlstelkomnetworkprod/raw/load_shedding_schedules"
    _clean_dbfs_path(load_shedding_parquet_path)
    distinct_days_ls = [
        (r["year"], r["month"], r["day"]) for r in load_shedding.select("year", "month", "day").distinct().collect()
    ]
    for y, m, d in distinct_days_ls:
        tmp_day_path = f"{load_shedding_parquet_path}/.tmp_day_{y}_{m}_{d}"
        (
            load_shedding.filter((col("year") == y) & (col("month") == m) & (col("day") == d))
            .coalesce(1)
            .write.format("parquet").option("header", "true").mode("overwrite").save(tmp_day_path)
        )
        tmp_local = tmp_day_path.replace("dbfs:", "/dbfs")
        root_local = load_shedding_parquet_path.replace("dbfs:", "/dbfs")
        if os.path.isdir(tmp_local):
            for fname in os.listdir(tmp_local):
                if fname.startswith("part-"):
                    os.replace(os.path.join(tmp_local, fname), os.path.join(root_local, fname))
            shutil.rmtree(tmp_local, ignore_errors=True)
    _rename_parts_in_folder(load_shedding_parquet_path)

    # =================== New datasets added below (preserving existing behavior) ===================

    # ---------- Customer Feedback ----------
    customer_feedback = (
        spark.range(200000)
        .select(
            expr(
                "element_at(array(" \
                "'Network is slow in my area'," \
                "'Frequent dropped calls'," \
                "'No coverage inside my building'," \
                "'High latency during peak hours'," \
                "'Billing issue with last invoice'," \
                "'SIM card not working'," \
                "'App keeps crashing'," \
                "'Unexpected charges on my account'," \
                "'Activation is delayed'," \
                "'Roaming does not work'" \
                "), cast(rand() * 10 as int) + 1)"
            ).alias("text"),
            expr("round(rand() * 2 - 1, 3)").alias("sentiment_score"),
            random_timestamp_expr().alias("timestamp"),
        )
        .withColumn(
            "sentiment_label",
            expr(
                "CASE WHEN sentiment_score < -0.2 THEN 'negative' WHEN sentiment_score > 0.2 THEN 'positive' ELSE 'neutral' END"
            ),
        )
        .withColumn("year", year(col("timestamp")))
        .withColumn("month", month(col("timestamp")))
        .withColumn("day", dayofmonth(col("timestamp")))
    )
    feedback_parquet_path = "dbfs:/mnt/dlstelkomnetworkprod/raw/customer_feedback"
    _clean_dbfs_path(feedback_parquet_path)
    distinct_days_cf = [
        (r["year"], r["month"], r["day"]) for r in customer_feedback.select("year", "month", "day").distinct().collect()
    ]
    for y, m, d in distinct_days_cf:
        tmp_day_path = f"{feedback_parquet_path}/.tmp_day_{y}_{m}_{d}"
        (
            customer_feedback.filter((col("year") == y) & (col("month") == m) & (col("day") == d))
            .coalesce(1)
            .write.format("parquet").mode("overwrite").save(tmp_day_path)
        )
        tmp_local = tmp_day_path.replace("dbfs:", "/dbfs")
        root_local = feedback_parquet_path.replace("dbfs:", "/dbfs")
        if os.path.isdir(tmp_local):
            for fname in os.listdir(tmp_local):
                if fname.startswith("part-"):
                    os.replace(os.path.join(tmp_local, fname), os.path.join(root_local, fname))
            shutil.rmtree(tmp_local, ignore_errors=True)
    _rename_parts_in_folder(feedback_parquet_path)

    # ---------- Tower Imagery ----------
    tower_imagery = (
        tower_locations_df.select(col("tower_id"), random_timestamp_expr().alias("timestamp"))
        .withColumn(
            "image_path",
            expr(
                "concat('dbfs:/mnt/dlstelkomnetworkprod/assets/tower_images/', tower_id, '/', cast(unix_timestamp(timestamp) as string), '.jpg')"
            ),
        )
        .withColumn(
            "condition_label",
            expr("element_at(array('OK','Minor','Critical'), cast(rand() * 3 as int) + 1)"),
        )
        .withColumn("year", year(col("timestamp")))
        .withColumn("month", month(col("timestamp")))
        .withColumn("day", dayofmonth(col("timestamp")))
    )
    imagery_parquet_path = "dbfs:/mnt/dlstelkomnetworkprod/raw/tower_imagery"
    _clean_dbfs_path(imagery_parquet_path)
    distinct_days_ti = [
        (r["year"], r["month"], r["day"]) for r in tower_imagery.select("year", "month", "day").distinct().collect()
    ]
    for y, m, d in distinct_days_ti:
        tmp_day_path = f"{imagery_parquet_path}/.tmp_day_{y}_{m}_{d}"
        (
            tower_imagery.filter((col("year") == y) & (col("month") == m) & (col("day") == d))
            .coalesce(1)
            .write.format("parquet").mode("overwrite").save(tmp_day_path)
        )
        tmp_local = tmp_day_path.replace("dbfs:", "/dbfs")
        root_local = imagery_parquet_path.replace("dbfs:", "/dbfs")
        if os.path.isdir(tmp_local):
            for fname in os.listdir(tmp_local):
                if fname.startswith("part-"):
                    os.replace(os.path.join(tmp_local, fname), os.path.join(root_local, fname))
            shutil.rmtree(tmp_local, ignore_errors=True)
    _rename_parts_in_folder(imagery_parquet_path)

    # ---------- Voice Transcriptions ----------
    voice_transcriptions = (
        spark.range(120000)
        .select(
            col("id").cast("string").alias("technician_id"),
            expr(
                "element_at(array(" \
                "'Start site inspection'," \
                "'Check signal levels'," \
                "'Replace faulty antenna'," \
                "'Confirm power status'," \
                "'Log maintenance complete'," \
                "'Escalate to network ops'," \
                "'Verify backhaul link'," \
                "'Capture tower imagery'," \
                "'Run diagnostic test'," \
                "'Close the ticket'" \
                "), cast(rand() * 10 as int) + 1)"
            ).alias("transcription"),
            random_timestamp_expr().alias("timestamp"),
        )
        .withColumn("year", year(col("timestamp")))
        .withColumn("month", month(col("timestamp")))
        .withColumn("day", dayofmonth(col("timestamp")))
    )
    vt_parquet_path = "dbfs:/mnt/dlstelkomnetworkprod/raw/voice_transcriptions"
    _clean_dbfs_path(vt_parquet_path)
    distinct_days_vt = [
        (r["year"], r["month"], r["day"]) for r in voice_transcriptions.select("year", "month", "day").distinct().collect()
    ]
    for y, m, d in distinct_days_vt:
        tmp_day_path = f"{vt_parquet_path}/.tmp_day_{y}_{m}_{d}"
        (
            voice_transcriptions.filter((col("year") == y) & (col("month") == m) & (col("day") == d))
            .coalesce(1)
            .write.format("parquet").mode("overwrite").save(tmp_day_path)
        )
        tmp_local = tmp_day_path.replace("dbfs:", "/dbfs")
        root_local = vt_parquet_path.replace("dbfs:", "/dbfs")
        if os.path.isdir(tmp_local):
            for fname in os.listdir(tmp_local):
                if fname.startswith("part-"):
                    os.replace(os.path.join(tmp_local, fname), os.path.join(root_local, fname))
            shutil.rmtree(tmp_local, ignore_errors=True)
    _rename_parts_in_folder(vt_parquet_path)

    # ---------- Tower Connectivity ----------
    neighbor_map = tower_locations_df.select(
        col("tower_id").alias("dst_tower_id"),
        expr("row_number() over (order by tower_id) - 1").alias("neighbor_index"),
    )
    src_with_neighbors = tower_locations_df.select(
        col("tower_id").alias("src_tower_id"),
        expr("row_number() over (order by tower_id) - 1").alias("tower_index"),
    )
    total_towers = neighbor_map.count()
    edges_idx = src_with_neighbors.select(
        col("src_tower_id"), expr(f"(tower_index + 1) % {total_towers}").alias("neighbor_index")
    ).union(
        src_with_neighbors.select(
            col("src_tower_id"), expr(f"(tower_index + 2) % {total_towers}").alias("neighbor_index")
        )
    )
    tower_connectivity = (
        edges_idx.join(neighbor_map, on="neighbor_index", how="left")
        .select(
            col("src_tower_id"),
            col("dst_tower_id"),
            (rand() * 100).alias("signal_quality"),
            random_timestamp_expr().alias("timestamp"),
        )
        .withColumn("year", year(col("timestamp")))
        .withColumn("month", month(col("timestamp")))
        .withColumn("day", dayofmonth(col("timestamp")))
    )
    connectivity_parquet_path = "dbfs:/mnt/dlstelkomnetworkprod/raw/tower_connectivity"
    _clean_dbfs_path(connectivity_parquet_path)
    distinct_days_tc = [
        (r["year"], r["month"], r["day"]) for r in tower_connectivity.select("year", "month", "day").distinct().collect()
    ]
    for y, m, d in distinct_days_tc:
        tmp_day_path = f"{connectivity_parquet_path}/.tmp_day_{y}_{m}_{d}"
        (
            tower_connectivity.filter((col("year") == y) & (col("month") == m) & (col("day") == d))
            .coalesce(1)
            .write.format("parquet").mode("overwrite").save(tmp_day_path)
        )
        tmp_local = tmp_day_path.replace("dbfs:", "/dbfs")
        root_local = connectivity_parquet_path.replace("dbfs:", "/dbfs")
        if os.path.isdir(tmp_local):
            for fname in os.listdir(tmp_local):
                if fname.startswith("part-"):
                    os.replace(os.path.join(tmp_local, fname), os.path.join(root_local, fname))
            shutil.rmtree(tmp_local, ignore_errors=True)
    _rename_parts_in_folder(connectivity_parquet_path)

    # ---------- Tower Capacity ----------
    tower_capacity = (
        tower_locations_df.select(
            col("tower_id"),
            (rand() * 900 + 100).alias("capacity_mbps"),
            (rand() * 80 + 10).alias("utilization_percent"),
            random_timestamp_expr().alias("timestamp"),
        )
        .withColumn("year", year(col("timestamp")))
        .withColumn("month", month(col("timestamp")))
        .withColumn("day", dayofmonth(col("timestamp")))
    )
    capacity_parquet_path = "dbfs:/mnt/dlstelkomnetworkprod/raw/tower_capacity"
    _clean_dbfs_path(capacity_parquet_path)
    distinct_days_tcap = [
        (r["year"], r["month"], r["day"]) for r in tower_capacity.select("year", "month", "day").distinct().collect()
    ]
    for y, m, d in distinct_days_tcap:
        tmp_day_path = f"{capacity_parquet_path}/.tmp_day_{y}_{m}_{d}"
        (
            tower_capacity.filter((col("year") == y) & (col("month") == m) & (col("day") == d))
            .coalesce(1)
            .write.format("parquet").mode("overwrite").save(tmp_day_path)
        )
        tmp_local = tmp_day_path.replace("dbfs:", "/dbfs")
        root_local = capacity_parquet_path.replace("dbfs:", "/dbfs")
        if os.path.isdir(tmp_local):
            for fname in os.listdir(tmp_local):
                if fname.startswith("part-"):
                    os.replace(os.path.join(tmp_local, fname), os.path.join(root_local, fname))
            shutil.rmtree(tmp_local, ignore_errors=True)
    _rename_parts_in_folder(capacity_parquet_path)

    # ---------- Maintenance Crew ----------
    maintenance_crew = (
        spark.range(5000)
        .select(
            col("id").cast("string").alias("crew_id"),
            expr(
                f"element_at(array({regions_sql_array}), cast(rand() * {len(regions)} as int) + 1)"
            ).alias("region"),
            (rand() * 7 + 22).alias("latitude"),
            (rand() * 9 + 16).alias("longitude"),
            expr("rand() < 0.7").alias("available"),
            random_timestamp_expr().alias("shift_start"),
        )
        .withColumn("shift_end", expr("timestampadd(HOUR, 8, shift_start)"))
        .withColumn("year", year(col("shift_start")))
        .withColumn("month", month(col("shift_start")))
        .withColumn("day", dayofmonth(col("shift_start")))
    )
    crew_parquet_path = "dbfs:/mnt/dlstelkomnetworkprod/raw/maintenance_crew"
    _clean_dbfs_path(crew_parquet_path)
    distinct_days_mc = [
        (r["year"], r["month"], r["day"]) for r in maintenance_crew.select("year", "month", "day").distinct().collect()
    ]
    for y, m, d in distinct_days_mc:
        tmp_day_path = f"{crew_parquet_path}/.tmp_day_{y}_{m}_{d}"
        (
            maintenance_crew.filter((col("year") == y) & (col("month") == m) & (col("day") == d))
            .coalesce(1)
            .write.format("parquet").mode("overwrite").save(tmp_day_path)
        )
        tmp_local = tmp_day_path.replace("dbfs:", "/dbfs")
        root_local = crew_parquet_path.replace("dbfs:", "/dbfs")
        if os.path.isdir(tmp_local):
            for fname in os.listdir(tmp_local):
                if fname.startswith("part-"):
                    os.replace(os.path.join(tmp_local, fname), os.path.join(root_local, fname))
            shutil.rmtree(tmp_local, ignore_errors=True)
    _rename_parts_in_folder(crew_parquet_path)

    # ---------- Upload to GitHub ----------
    def upload_all_files_from_folder(dbfs_folder, github_folder):
        """Recursively upload all Spark part files, preserving partition directories.
        Maintains per-folder part indexing in GitHub to avoid conflicts."""
        root_local = dbfs_folder.replace("dbfs:", "/dbfs")
        for dirpath, _, filenames in os.walk(root_local):
            part_files = sorted([fn for fn in filenames if fn.startswith("part-")])
            if not part_files:
                continue

            rel_dir = os.path.relpath(dirpath, root_local).replace("\\", "/")
            gh_subfolder = f"{github_folder}/{rel_dir}" if rel_dir != "." else github_folder

            for file in part_files:
                # Use the exact local filename when uploading to GitHub
                local_path = os.path.join(dirpath, file)
                github_path = f"{gh_subfolder}/{file}"
                upload_to_github(local_path, github_path, commit_msg)

    # Existing datasets
    upload_all_files_from_folder(tower_parquet_path, "data/tower_locations")
    upload_all_files_from_folder(network_parquet_path, "data/network_logs")
    upload_all_files_from_folder(weather_parquet_path, "data/weather_data")
    upload_all_files_from_folder(customer_parquet_path, "data/customer_usage")
    upload_all_files_from_folder(load_shedding_parquet_path, "data/load_shedding_schedules")

    # New datasets
    upload_all_files_from_folder(feedback_parquet_path, "data/customer_feedback")
    upload_all_files_from_folder(imagery_parquet_path, "data/tower_imagery")
    upload_all_files_from_folder(vt_parquet_path, "data/voice_transcriptions")
    upload_all_files_from_folder(connectivity_parquet_path, "data/tower_connectivity")
    upload_all_files_from_folder(capacity_parquet_path, "data/tower_capacity")
    upload_all_files_from_folder(crew_parquet_path, "data/maintenance_crew")

    return True


if __name__ == "__main__":
    generate_timestamp_data()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    rand, col, expr, when, year, month, dayofmonth, to_date, lit, format_string,
    hash as spark_hash, pmod
)
import requests
import base64
import os
import shutil
from datetime import datetime

# GitHub configuration
GITHUB_TOKEN = dbutils.secrets.get(scope="databricksazure", key="github-pat-token")
GITHUB_REPO = "fusionoasis/analytics-telkom"
GITHUB_API_URL = f"https://api.github.com/repos/{GITHUB_REPO}/contents"

spark = SparkSession.builder.getOrCreate()

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
    """Upload a single file to GitHub repository"""
    with open(file_path.replace("dbfs:", "/dbfs"), "rb") as f:
        content = f.read()

    content_encoded = base64.b64encode(content).decode()

    payload = {
        "message": commit_message,
        "content": content_encoded
    }

    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }

    response = requests.put(f"{GITHUB_API_URL}/{github_path}", json=payload, headers=headers)

    if response.status_code in [200, 201]:
        return True
    else:
        return False


def random_timestamp_expr():
    """Generate random timestamp between 2025-07-01 and 2025-08-19"""
    return expr("""
        timestampadd(
            SECOND,
            cast(rand() * 86400 as int),
            date_add(to_date('2025-07-01'), cast(rand() * 50 as int))
        )
    """)


def generate_timestamp_data():
    current_time = datetime.now()
    commit_msg = f"Data update {current_time.strftime('%Y%m%d_%H%M%S')}"

    regions = [
        "Gauteng", "KwaZulu-Natal", "Western Cape", "Eastern Cape", "Free State",
        "Mpumalanga", "Northern Cape", "Limpopo", "North West"
    ]
    regions_sql_array = ", ".join([f"'{r}'" for r in regions])

    # ---------- Tower Locations ----------
    tower_locations = spark.range(15000).select(
        col("id").cast("string").alias("tower_id"),
        (rand() * 7 + 22).alias("latitude"),
        (rand() * 9 + 16).alias("longitude")
    ).withColumn(
        "region_index", pmod(spark_hash(col("tower_id")), lit(len(regions))).cast("int")
    ).withColumn(
        "region", expr(f"element_at(array({regions_sql_array}), region_index + 1)")
    )
    tower_parquet_path = "dbfs:/mnt/dlstelkomnetworkprod/raw/tower_locations"
    tower_locations.coalesce(1).write.format("parquet").mode("overwrite").option("header", "true").save(tower_parquet_path)

    tower_locations_df = tower_locations.withColumn("tower_index", expr("row_number() over (order by tower_id) - 1"))

    # ---------- Network Logs ----------
    network_logs = spark.range(6000000).select(
        (rand() * tower_locations_df.count()).cast("int").alias("tower_index")
    ).join(
        tower_locations_df,
        on="tower_index"
    ).select(
        col("tower_id"),
        (rand() * 100).alias("signal_strength"),
        (rand() * 10).alias("latency_ms"),
        random_timestamp_expr().alias("timestamp"),
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
        "region_index", pmod(spark_hash(col("tower_id")), lit(len(regions))).cast("int")
    ).withColumn(
        "region", expr(f"element_at(array({regions_sql_array}), region_index + 1)")
    ).filter(
        (col("year") == 2025) &
        (col("timestamp") >= "2025-07-01 00:00:00") &
        (col("timestamp") <= "2025-08-19 23:59:59")
    )
    network_parquet_path = "dbfs:/mnt/dlstelkomnetworkprod/raw/network_logs"
    def _clean_dbfs_path(dbfs_path: str):
        local = dbfs_path.replace("dbfs:", "/dbfs")
        if os.path.exists(local):
            shutil.rmtree(local)

    _clean_dbfs_path(network_parquet_path)
    distinct_days_nw = [
        (r["year"], r["month"], r["day"]) for r in network_logs.select("year", "month", "day").distinct().collect()
    ]
    for y, m, d in distinct_days_nw:
        day_path = f"{network_parquet_path}/year={y}/month={m}/day={d}"
        (network_logs
         .filter((col("year") == y) & (col("month") == m) & (col("day") == d))
         .coalesce(1)
         .write.format("parquet").mode("overwrite").save(day_path)
        )

    # ---------- Weather Data ----------
    tower_dates = network_logs.select("tower_id", "timestamp").distinct()
    weather_data = tower_dates.withColumn(
        "temperature_c", (rand() * 15 + 20).cast("double")
    ).withColumn(
        "humidity_percent", (rand() * 40 + 40).cast("double")
    ).withColumn(
        "wind_speed_mps", (rand() * 10).cast("double")
    ).withColumn(
        "weather_condition", expr(
            "CASE WHEN rand() < 0.2 THEN 'Rain' WHEN rand() < 0.5 THEN 'Clouds' ELSE 'Clear' END"
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
        "region_index", pmod(spark_hash(col("tower_id")), lit(len(regions))).cast("int")
    ).withColumn(
        "region", expr(f"element_at(array({regions_sql_array}), region_index + 1)")
    )
    weather_parquet_path = "dbfs:/mnt/dlstelkomnetworkprod/raw/weather_data"
    _clean_dbfs_path(weather_parquet_path)
    distinct_days_w = [
        (r["year"], r["month"], r["day"]) for r in weather_data.select("year", "month", "day").distinct().collect()
    ]
    for y, m, d in distinct_days_w:
        day_path = f"{weather_parquet_path}/year={y}/month={m}/day={d}"
        (weather_data
         .filter((col("year") == y) & (col("month") == m) & (col("day") == d))
         .coalesce(1)
         .write.format("parquet").option("header", "true").mode("overwrite").save(day_path)
        )

    # ---------- Customer Usage ----------
    customer_usage = spark.range(6000000).select(
        col("id").cast("string").alias("customer_id"),
        (rand() * 1000).alias("data_usage_mb"),
        (rand() * 60).alias("call_duration_min"),
        random_timestamp_expr().alias("timestamp")
    ).withColumn(
        "data_usage_mb",
        when(col("data_usage_mb") > 800, col("data_usage_mb") * 1.2).otherwise(col("data_usage_mb"))
    ).filter(
        (year(col("timestamp")) == 2025) &
        (col("timestamp") >= "2025-07-01 00:00:00") &
        (col("timestamp") <= "2025-08-19 23:59:59")
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
    customer_parquet_path = "dbfs:/mnt/dlstelkomnetworkprod/raw/customer_usage"
    _clean_dbfs_path(customer_parquet_path)
    distinct_days_cu = [
        (r["year"], r["month"], r["day"]) for r in customer_usage.select("year", "month", "day").distinct().collect()
    ]
    for y, m, d in distinct_days_cu:
        day_path = f"{customer_parquet_path}/year={y}/month={m}/day={d}"
        (customer_usage
         .filter((col("year") == y) & (col("month") == m) & (col("day") == d))
         .coalesce(1)
         .write.format("parquet").mode("overwrite").save(day_path)
        )

    # ---------- Load Shedding Schedules ----------
    # Generate synthetic schedules with region, start_time, end_time
    load_shedding = spark.range(20000).select(
        expr(f"element_at(array({regions_sql_array}), cast(rand() * {len(regions)} as int) + 1)").alias("region"),
        random_timestamp_expr().alias("start_time")
    ).withColumn(
        "end_time", expr("timestampadd(HOUR, cast(rand() * 3 + 1 as int), start_time)")
    ).withColumn(
        "year", year(col("start_time"))
    ).withColumn(
        "month", month(col("start_time"))
    ).withColumn(
        "day", dayofmonth(col("start_time"))
    )
    load_shedding_parquet_path = "dbfs:/mnt/dlstelkomnetworkprod/raw/load_shedding_schedules"
    _clean_dbfs_path(load_shedding_parquet_path)
    distinct_days_ls = [
        (r["year"], r["month"], r["day"]) for r in load_shedding.select("year", "month", "day").distinct().collect()
    ]
    for y, m, d in distinct_days_ls:
        day_path = f"{load_shedding_parquet_path}/year={y}/month={m}/day={d}"
        (load_shedding
         .filter((col("year") == y) & (col("month") == m) & (col("day") == d))
         .coalesce(1)
         .write.format("parquet").option("header", "true").mode("overwrite").save(day_path)
        )

    # ---------- Upload to GitHub ----------
    def upload_all_files_from_folder(dbfs_folder, github_folder):
        """Recursively upload all Spark part files, preserving partition directories.
        Maintains per-folder part indexing in GitHub to avoid conflicts.
        """
        root_local = dbfs_folder.replace("dbfs:", "/dbfs")
        for dirpath, _, filenames in os.walk(root_local):
            part_files = sorted([fn for fn in filenames if fn.startswith("part-")])
            if not part_files:
                continue

            # Determine GitHub subfolder for this partition directory
            rel_dir = os.path.relpath(dirpath, root_local).replace("\\", "/")
            gh_subfolder = f"{github_folder}/{rel_dir}" if rel_dir != "." else github_folder

            # Get last index used in this GitHub subfolder
            last_idx = get_last_index_from_github(gh_subfolder)
            next_idx = last_idx + 1

            for file in part_files:
                file_ext = os.path.splitext(file)[1]
                new_name = f"part-{next_idx:05d}-tid{file_ext}"

                old_path = os.path.join(dirpath, file)
                new_path = os.path.join(dirpath, new_name)

                # Rename locally in DBFS
                os.rename(old_path, new_path)

                # Upload to GitHub under the corresponding subfolder
                github_path = f"{gh_subfolder}/{new_name}"
                upload_to_github(new_path, github_path, commit_msg)

                next_idx += 1

    upload_all_files_from_folder(tower_parquet_path, "data/tower_locations")
    upload_all_files_from_folder(network_parquet_path, "data/network_logs")
    upload_all_files_from_folder(weather_parquet_path, "data/weather_data")
    upload_all_files_from_folder(customer_parquet_path, "data/customer_usage")
    upload_all_files_from_folder(load_shedding_parquet_path, "data/load_shedding_schedules")

    return True


if __name__ == "__main__":
    generate_timestamp_data()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    rand, col, expr, when, monotonically_increasing_id,
    year, month, dayofmonth, to_timestamp, lit, hash as spark_hash, pmod
)
import os
import shutil
from datetime import datetime, timedelta

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

# === Storage base (ABFSS) ===
RAW_BASE = "abfss://raw@dlstelkomnetworkprod.dfs.core.windows.net"

# === Volume controls (can be tuned via env vars) ===
SCALE = float(os.getenv("DATA_SCALE", "1.0"))

def _scale(n: int) -> int:
    try:
        return max(1, int(n * SCALE))
    except Exception:
        return n

# === Utility: SQL array picker for categorical values ===
def pick_array_str(arr):
    return f"element_at(array({', '.join([repr(a) for a in arr])}), cast(rand() * {len(arr)} as int) + 1)"

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

def _exists(path: str) -> bool:
    """Return True if the given abfss/dbfs path exists and has any content."""
    try:
        files = dbutils.fs.ls(path)
        return len(files) > 0
    except Exception:
        return False

def _truthy(val: str) -> bool:
    return isinstance(val, str) and val.lower() in ("1", "true", "yes", "y", "on")

def _should_refresh_master(name: str) -> bool:
    """Decide if a master dataset should be regenerated.
    Triggers:
      - REFRESH_MASTERS=1 (env)
      - REFRESH_<NAME>=1 (env)
      - Control path exists at {RAW_BASE}/_control/<name>.refresh
    """
    if _truthy(os.getenv("REFRESH_MASTERS", "")):
        return True
    if _truthy(os.getenv(f"REFRESH_{name.upper()}", "")):
        return True
    ctrl_path = f"{RAW_BASE}/_control/{name}.refresh"
    return _exists(ctrl_path)

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

 

# === Tower Data Generation ===
def generate_tower_locations_once():
    """Generate Tower Locations with full schema (only needed once)."""
    provinces = [
        "Gauteng", "KwaZulu-Natal", "Western Cape", "Eastern Cape", "Free State",
        "Mpumalanga", "Northern Cape", "Limpopo", "North West"
    ]
    provinces_sql_array = ", ".join([f"'{r}'" for r in provinces])

    tower_locations = (
        spark.range(_scale(15000))
        .select(
            col("id").cast("string").alias("tower_id"),
            expr("concat('Tower-', lpad(cast(id as string), 5, '0'))").alias("site_name"),
            (rand() * 7 + 22).alias("latitude"),
            (rand() * 9 + 16).alias("longitude"),
            expr("cast(rand() * 1500 + 200 as int)").alias("altitude_meters"),
            expr(pick_array_str(["Macro", "Micro", "Pico", "Femto"])) .alias("tower_type"),
            expr("timestampadd(DAY, - cast(rand() * 3650 as int), current_timestamp())").alias("installation_date"),
        )
        .withColumn("province_index", pmod(spark_hash(col("tower_id")), lit(len(provinces))).cast("int"))
        .withColumn("province", expr(f"element_at(array({provinces_sql_array}), province_index + 1)"))
        .withColumn("municipality", expr("concat(province, ' Municipality ', cast((rand()*50)+1 as int))"))
        .withColumn("suburb", expr("concat('Suburb ', cast((rand()*500)+1 as int))"))
        .withColumn("address", expr("concat(cast(cast(rand()*999 as int) as string), ' Main St, ', suburb)") )
        .withColumn("land_ownership", expr(pick_array_str(["Owned", "Leased", "Rooftop"])) )
        .withColumn("power_source", expr(pick_array_str(["Grid", "Solar", "Hybrid", "Battery"])) )
        .withColumn("backup_power_hours", expr("cast(rand()*72 as int)"))
        .withColumn("fiber_connectivity", expr("rand() < 0.6"))
        .withColumn("microwave_connectivity", expr("rand() < 0.5"))
        .withColumn("satellite_connectivity", expr("rand() < 0.1"))
        .withColumn("access_road_condition", expr(pick_array_str(["Good", "Fair", "Poor", "None"])) )
        .withColumn("security_level", expr(pick_array_str(["High", "Medium", "Low"])) )
        .withColumn("maintenance_contractor", expr(pick_array_str(["Contractor A", "Contractor B", "Contractor C"])) )
        .withColumn("lease_expiry_date", expr("timestampadd(DAY, cast(rand()*2000 as int), current_timestamp())"))
        .withColumn("tower_height_meters", expr("cast(rand()*60 + 20 as int)"))
        .withColumn("foundation_type", expr(pick_array_str(["Concrete", "Steel", "Other"])) )
        .withColumn("environmental_zone", expr(pick_array_str(["Urban", "Suburban", "Rural", "Remote"])) )
        .withColumn("population_density_category", expr(pick_array_str(["High", "Medium", "Low"])) )
    )

    tower_path = f"{RAW_BASE}/tower_locations"
    _clean_dbfs_path(tower_path)
    tmp_tower = f"{tower_path}/.tmp_init_{int(datetime.now().timestamp())}"
    tower_locations.coalesce(1).write.format("parquet").mode("overwrite").save(tmp_tower)
    _move_single_part_from_tmp(tmp_tower, tower_path)
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
    """Generate incremental data for the previous 4-hour interval and upload one file per dataset."""
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=4)

    # Load or refresh Tower Locations (master)
    tower_path = f"{RAW_BASE}/tower_locations"
    if _should_refresh_master("tower_locations") or not _exists(tower_path):
        tower_locations = generate_tower_locations_once()
        # optional: clear control flag path if used
        _rm_path(f"{RAW_BASE}/_control/tower_locations.refresh")
    else:
        tower_locations = spark.read.format("parquet").load(tower_path)
        tower_locations = tower_locations.limit(_scale(15000))

    tower_locations_df = tower_locations.select("tower_id").withColumn("tower_index", expr("row_number() over (order by tower_id) - 1"))
    towers_index_lookup = tower_locations_df.select("tower_index", "tower_id")
    total_towers = towers_index_lookup.count()
    towers_by_province = (
        tower_locations.select("province", "tower_id")
        .withColumn("province_rank", expr("row_number() over (partition by province order by tower_id)"))
    )
    province_counts = tower_locations.groupBy("province").count().withColumnRenamed("count", "province_tower_count")

    # Helper for daily path tmp write/move
    def write_single(df, path):
        tmp = f"{path}/.tmp_interval_{int(end_time.timestamp())}"
        df.coalesce(1).write.format("parquet").mode("overwrite").save(tmp)
        return _move_single_part_from_tmp(tmp, path)

    # ========== Network Equipment Inventory (master; load-or-create) ==========
    manufacturers = ["Nokia", "Huawei", "Ericsson", "ZTE", "Cisco", "Juniper"]
    equip_types = ["BTS", "RRU", "Antenna", "Router", "Switch", "Power"]
    techs = ["2G", "3G", "4G", "5G"]
    freqs = [700, 800, 900, 1800, 2100, 2600, 3500]

    # Generate ~3 devices per tower on average (scaled)
    equipment_path = f"{RAW_BASE}/network_equipment_inventory"
    if _should_refresh_master("network_equipment_inventory") or not _exists(equipment_path):
        eq_rows = _scale(45000)
        equipment_inventory = (
            spark.range(eq_rows)
            .withColumn("tower_index", pmod(spark_hash(col("id")), lit(total_towers)).cast("int"))
            .join(towers_index_lookup, on="tower_index", how="left")
            .select(
                expr("concat('EQ-', lpad(cast(id as string), 8, '0'))").alias("equipment_id"),
                col("tower_id"),
                expr(f"{pick_array_str(equip_types)}").alias("equipment_type"),
                expr(f"{pick_array_str(manufacturers)}").alias("manufacturer"),
                expr("concat('M', lpad(cast(cast(rand()*9999 as int) as string), 4, '0'))").alias("model"),
                expr("concat('SN', lpad(cast(cast(rand()*99999999 as int) as string), 8, '0'))").alias("serial_number"),
                expr("concat('FW', cast(cast(rand()*10 as int) as string), '.', cast(cast(rand()*10 as int) as string))").alias("firmware_version"),
                expr("concat('HW', cast(cast(rand()*5 as int) as string))").alias("hardware_version"),
                random_timestamp_in_interval(start_time - timedelta(days=180), end_time - timedelta(days=1)).alias("installation_date"),
                random_timestamp_in_interval(end_time, end_time + timedelta(days=365)).alias("warranty_expiry_date"),
                random_timestamp_in_interval(start_time - timedelta(days=30), end_time).alias("last_maintenance_date"),
                expr(pick_array_str(["Quarterly", "Semi-annual", "Annual"])) .alias("maintenance_schedule"),
                expr(pick_array_str(["Active", "Inactive", "Maintenance", "Failed"])) .alias("operational_status"),
                expr("cast(rand()*800 + 50 as int)").alias("power_consumption_watts"),
                expr(f"element_at(array({', '.join(map(str, freqs))}), cast(rand()*{len(freqs)} as int) + 1)").alias("operating_frequency_mhz"),
                expr("cast(rand()*1000 + 100 as int)").alias("bandwidth_capacity_mbps"),
                expr(pick_array_str(techs)).alias("technology_standard"),
                expr("cast(rand()*2000 + 200 as int)").alias("coverage_radius_meters"),
                expr("round(rand()*20, 2)").alias("antenna_gain_dbi"),
                expr("round(rand()*30 + 10, 2)").alias("transmit_power_dbm"),
                expr("round(-1 * (rand()*110 + 50), 2)").alias("receive_sensitivity_dbm"),
                expr("cast(rand()*40 - 20 as int)").alias("temperature_rating_min_celsius"),
                expr("cast(rand()*40 + 40 as int)").alias("temperature_rating_max_celsius"),
                expr("cast(rand()*100 as int)").alias("humidity_rating_percent"),
                expr("concat('10.', cast(cast(rand()*255 as int) as string), '.', cast(cast(rand()*255 as int) as string), '.', cast(cast(rand()*255 as int) as string))").alias("ip_address"),
                expr("concat('public', cast(cast(rand()*10 as int) as string))").alias("snmp_community"),
            )
        )
        write_single(equipment_inventory, equipment_path)
        _rm_path(f"{RAW_BASE}/_control/network_equipment_inventory.refresh")
    else:
        equipment_inventory = spark.read.format("parquet").load(equipment_path)

    # ========== Network Performance Telemetry (5-min bins in last 4h) ==========
    eq_for_telemetry = equipment_inventory.select("equipment_id", "tower_id").limit(_scale(1500))
    # Generate ~48 or fewer 5-min intervals
    intervals = spark.range(48).select(expr(f"timestampadd(MINUTE, -5 * cast(id as int), to_timestamp('{end_time.strftime('%Y-%m-%d %H:%M:%S')}'))").alias("timestamp")).filter(col("timestamp") >= to_timestamp(lit(start_time.strftime('%Y-%m-%d %H:%M:%S'))))
    telemetry = (
        eq_for_telemetry.crossJoin(intervals)
        .select(
            col("timestamp"),
            col("equipment_id"),
            col("tower_id"),
            expr("round(rand()*100, 2)").alias("cpu_utilization_percent"),
            expr("round(rand()*100, 2)").alias("memory_utilization_percent"),
            expr("round(rand()*100, 2)").alias("disk_utilization_percent"),
            expr("round(rand()*30 + 20, 2)").alias("temperature_celsius"),
            expr("round(rand()*60 + 20, 2)").alias("humidity_percent"),
            expr("round(rand()*500 + 50, 2)").alias("power_consumption_watts"),
            expr("round(rand()*240 + 180, 2)").alias("input_voltage_volts"),
            expr("round(-1*(rand()*60 + 40), 2)").alias("signal_strength_dbm"),
            expr("round(rand()*100, 2)").alias("signal_quality_percent"),
            expr("round(-1*(rand()*30 + 60), 2)").alias("noise_level_dbm"),
            expr("round(rand()*1e-5, 7)").alias("bit_error_rate"),
            expr("round(rand()*1e-3, 6)").alias("frame_error_rate"),
            expr("round(rand()*5, 3)").alias("packet_loss_rate_percent"),
            expr("round(rand()*50 + 5, 2)").alias("round_trip_time_ms"),
            expr("round(rand()*100 + 10, 2)").alias("throughput_upload_mbps"),
            expr("round(rand()*200 + 20, 2)").alias("throughput_download_mbps"),
            expr("cast(rand()*1000 as int)").alias("active_connections_count"),
            expr("cast(rand()*50 as int)").alias("dropped_connections_count"),
            expr("round(rand()*100, 2)").alias("bandwidth_utilization_percent"),
            expr("cast(rand()*10 as int)").alias("interface_errors_count"),
            expr("cast(rand()*100 as int)").alias("retransmission_count"),
            expr("cast(rand()*1e7 as int)").alias("uptime_seconds"),
            expr(pick_array_str(["Critical", "Major", "Minor", "Clear"])) .alias("alarm_status"),
            expr("concat('AL', lpad(cast(cast(rand()*9999 as int) as string), 4, '0'))").alias("alarm_code"),
            expr(pick_array_str(["Reachable", "Unreachable", "Timeout"])) .alias("snmp_status"),
        )
    )
    write_single(telemetry, f"{RAW_BASE}/network_performance_telemetry")

    # ========== Customer Usage (hourly aggregates) ==========
    service_types = ["Voice", "SMS", "Data", "Video"]
    technologies = ["2G", "3G", "4G", "5G"]
    subscriber_category = ["Prepaid", "Postpaid", "Corporate"]
    device_type = ["Smartphone", "Feature", "Tablet", "Router"]
    app_category = ["Social", "Streaming", "Browsing", "Gaming"]
    qos_classes = ["Premium", "Standard", "Basic"]

    customer_usage = (
        spark.range(_scale(100000))
        .select(
            random_timestamp_in_interval(start_time, end_time).alias("timestamp"),
            expr("concat('CUST', lpad(cast(id as string), 8, '0'))").alias("anonymized_customer_id"),
            expr(pick_array_str(service_types)).alias("service_type"),
            expr(pick_array_str(technologies)).alias("technology_used"),
            expr("cast(rand()*3600 as int)").alias("session_duration_seconds"),
            expr("cast(rand()*1e9 as bigint)").alias("data_volume_bytes"),
            expr("cast(rand()*3600 as int)").alias("call_duration_seconds"),
            expr("cast(rand()*10 as int)").alias("sms_count"),
            expr("cast(rand()*5 as int)").alias("connection_attempts"),
            expr("cast(rand()*5 as int)").alias("successful_connections"),
            expr("cast(rand()*2 as int)").alias("dropped_calls_count"),
            expr("cast(rand()*3 as int)").alias("handover_count"),
            expr("rand() < 0.05").alias("roaming_status"),
            expr(pick_array_str(subscriber_category)).alias("subscriber_category"),
            expr(pick_array_str(device_type)).alias("device_type"),
            expr(pick_array_str(["Samsung", "Apple", "Huawei", "Nokia", "Xiaomi"])) .alias("device_manufacturer"),
            expr("concat('Model ', cast(cast(rand()*50 as int) as string))").alias("device_model"),
            expr(pick_array_str(["iOS", "Android", "Other"])) .alias("operating_system"),
            expr(pick_array_str(app_category)).alias("application_category"),
            expr(pick_array_str(qos_classes)).alias("qos_class"),
            expr("concat('LAC', lpad(cast(cast(rand()*9999 as int) as string), 4, '0'))").alias("location_area_code"),
            expr("concat('CELL', lpad(cast(cast(rand()*999999 as int) as string), 6, '0'))").alias("cell_id"),
            expr("concat('SEC', lpad(cast(cast(rand()*999 as int) as string), 3, '0'))").alias("sector_id"),
        )
        .withColumn("tower_index", pmod(spark_hash(col("anonymized_customer_id")), lit(total_towers)).cast("int"))
        .join(towers_index_lookup, on="tower_index", how="left")
        .drop("tower_index")
    )
    write_single(customer_usage.select(
        "timestamp","tower_id","anonymized_customer_id","service_type","technology_used","session_duration_seconds","data_volume_bytes","call_duration_seconds","sms_count","connection_attempts","successful_connections","dropped_calls_count","handover_count","roaming_status","subscriber_category","device_type","device_manufacturer","device_model","operating_system","application_category","qos_class","location_area_code","cell_id","sector_id"
    ), f"{RAW_BASE}/customer_usage")

    # ========== Weather Data (hourly) ==========
    stations = (
        spark.range(_scale(200))
        .select(
            expr("concat('WS', lpad(cast(id as string), 5, '0'))").alias("weather_station_id"),
            (rand() * 7 + 22).alias("latitude"),
            (rand() * 9 + 16).alias("longitude"),
        )
    )
    hours = spark.range(24).select(expr(f"timestampadd(HOUR, - cast(id as int), to_timestamp('{end_time.strftime('%Y-%m-%d %H:%M:%S')}'))").alias("timestamp")).filter(col("timestamp") >= to_timestamp(lit(start_time.strftime('%Y-%m-%d %H:%M:%S'))))
    weather_data = (
        stations.crossJoin(hours)
        .select(
            col("timestamp"),
            col("weather_station_id"),
            col("latitude"),
            col("longitude"),
            expr("round(rand()*20 + 10, 2)").alias("temperature_celsius"),
            expr("round(rand()*60 + 20, 2)").alias("humidity_percent"),
            expr("round(rand()*30 + 980, 2)").alias("atmospheric_pressure_hpa"),
            expr("round(rand()*60, 2)").alias("wind_speed_kmh"),
            expr("cast(rand()*360 as int)").alias("wind_direction_degrees"),
            expr("round(rand()*80, 2)").alias("wind_gust_speed_kmh"),
            expr("round(rand()*10, 2)").alias("precipitation_mm"),
            expr(pick_array_str(["Rain", "Snow", "Hail", "None"])) .alias("precipitation_type"),
            expr("round(rand()*20, 2)").alias("visibility_km"),
            expr("cast(rand()*100 as int)").alias("cloud_cover_percent"),
            expr("round(rand()*11, 2)").alias("uv_index"),
            expr("round(rand()*1200, 2)").alias("solar_radiation_wm2"),
            expr("round(rand()*20, 2)").alias("dew_point_celsius"),
            expr("round(rand()*40, 2)").alias("heat_index_celsius"),
            expr(pick_array_str(["Clear", "Cloudy", "Rainy", "Stormy"])) .alias("weather_condition"),
            expr("rand() < 0.02").alias("severe_weather_alert"),
            expr("rand() < 0.05").alias("lightning_detected"),
            expr("round(rand()*10, 2)").alias("hail_size_mm"),
            expr("rand() < 0.01").alias("tornado_warning"),
            expr("rand() < 0.03").alias("flood_warning"),
            expr(pick_array_str(["Low", "Moderate", "High", "Extreme"])) .alias("fire_danger_rating"),
        )
    )
    write_single(weather_data, f"{RAW_BASE}/weather_data")

    # ========== Customer Service Records ==========
    complaint_cat = ["Network", "Billing", "Device", "Service"]
    priority_levels = ["Low", "Medium", "High", "Critical"]
    channels = ["Phone", "Email", "SMS", "App", "Store"]
    closure_codes = ["Resolved", "Escalated", "Duplicate", "No Fault Found"]
    affected_services = ["Voice", "Data", "SMS", "All"]
    root_cause = ["Technical", "Process", "External"]
    csr_base = (
        spark.range(_scale(20000))
        .select(
            expr("concat('TCK', lpad(cast(id as string), 9, '0'))").alias("ticket_id"),
            random_timestamp_in_interval(start_time, end_time).alias("timestamp"),
            expr("concat('CUST', lpad(cast(id as string), 8, '0'))").alias("anonymized_customer_id"),
            expr("concat('+27-***-***-', lpad(cast(cast(rand()*9999 as int) as string), 4, '0'))").alias("customer_phone_number_masked"),
            expr("concat('Area ', cast(cast(rand()*999 as int) as string))").alias("service_address_area"),
            expr(pick_array_str(complaint_cat)).alias("complaint_category"),
            expr("concat('Subcat ', cast(cast(rand()*20 as int) as string))").alias("complaint_subcategory"),
            expr(pick_array_str(priority_levels)).alias("priority_level"),
            expr(pick_array_str(channels)).alias("channel"),
            expr("concat('AG', lpad(cast(cast(rand()*99999 as int) as string), 5, '0'))").alias("agent_id"),
            expr("cast(rand()*1440 as int)").alias("resolution_time_minutes"),
            expr("cast(rand()*5 + 1 as int)").alias("customer_satisfaction_score"),
            expr("rand() < 0.3").alias("repeat_customer"),
            expr("rand() < 0.2").alias("escalated"),
            expr("rand() < 0.85").alias("resolved"),
            expr(pick_array_str(closure_codes)).alias("closure_code"),
            expr(pick_array_str(affected_services)).alias("affected_services"),
        )
        .withColumn("complaint_description", expr("concat('Issue desc ', cast(id as string))"))
        .withColumn("resolution_description", expr("concat('Resolution notes ', cast(id as string))"))
        .withColumn("followup_required", expr("rand() < 0.1"))
        .withColumn("compensation_provided", expr("round(rand()*200, 2)"))
        .withColumn("root_cause_category", expr(pick_array_str(root_cause)))
    )
    csr = (
        csr_base
        .withColumn("tower_index", pmod(spark_hash(col("ticket_id")), lit(total_towers)).cast("int"))
        .join(towers_index_lookup, on="tower_index", how="left")
        .withColumn("suspected_tower_id", when(rand() < 0.5, col("tower_id")).otherwise(lit(None)))
        .drop("tower_index", "tower_id")
    )
    write_single(csr, f"{RAW_BASE}/customer_service_records")

    # ========== Voice Transcriptions ==========
    voice_transcriptions = (
        spark.range(_scale(6000))
        .select(
            col("id").cast("string").alias("technician_id"),
            random_timestamp_in_interval(start_time, end_time).alias("timestamp"),
            expr("concat('abfss://raw@dlstelkomnetworkprod.dfs.core.windows.net/assets/audio/', lpad(cast(id as string), 8, '0'), '.wav')").alias("audio_file_path"),
            expr("element_at(array('Start site inspection','Check signal levels','Replace faulty antenna','Confirm power status','Log maintenance complete','Escalate to network ops','Verify backhaul link','Capture tower imagery','Run diagnostic test','Close the ticket'), cast(rand()*10 as int)+1)").alias("transcription_raw"),
            expr("cast(rand()*600 as int)").alias("audio_duration_seconds"),
            expr("round(rand()*100, 2)").alias("audio_quality_score"),
            expr("element_at(array('en','af','zu','xh'), cast(rand()*4 as int)+1)").alias("language_detected"),
            expr("round(rand(), 3)").alias("confidence_score"),
            expr("cast(rand()*3 as int) + 1").alias("speaker_count"),
            expr("element_at(array('Low','Medium','High'), cast(rand()*3 as int)+1)").alias("background_noise_level"),
            expr("element_at(array('Phone','Bodycam','Radio'), cast(rand()*3 as int)+1)").alias("recording_device"),
            expr("concat('WO', lpad(cast(cast(rand()*999999 as int) as string), 6, '0'))").alias("work_order_id"),
            expr("element_at(array('Indoor','Outdoor','Vehicle'), cast(rand()*3 as int)+1)").alias("recording_location"),
            expr("rand() < 0.02").alias("emergency_flag"),
        )
        .withColumn("technician_id", expr("concat('TECH', lpad(technician_id, 6, '0'))"))
        .withColumn("tower_index", pmod(spark_hash(col("technician_id")), lit(total_towers)).cast("int"))
        .join(towers_index_lookup, on="tower_index", how="left")
        .drop("tower_index")
    )
    write_single(voice_transcriptions.select("technician_id","timestamp","audio_file_path","transcription_raw","audio_duration_seconds","audio_quality_score","language_detected","confidence_score","speaker_count","background_noise_level","recording_device","tower_id","work_order_id","recording_location","emergency_flag"), f"{RAW_BASE}/voice_transcriptions")

    # ========== Competitor Analysis Data (small daily sample) ==========
    comp = (
        spark.range(_scale(2000))
        .select(
            random_timestamp_in_interval(start_time - timedelta(days=7), end_time).alias("measurement_date"),
            expr("element_at(array('Vodacom','MTN','Cell C','Rain'), cast(rand()*4 as int)+1)").alias("competitor_name"),
            (rand()*7 + 22).alias("measurement_location_lat"),
            (rand()*9 + 16).alias("measurement_location_lng"),
            expr(pick_array_str(["Urban","Suburban","Rural"])) .alias("area_classification"),
            expr(pick_array_str(["2G","3G","4G","5G"])) .alias("technology_tested"),
            expr("round(rand()*200 + 10, 2)").alias("download_speed_mbps"),
            expr("round(rand()*100 + 5, 2)").alias("upload_speed_mbps"),
            expr("round(rand()*100 + 5, 2)").alias("latency_ms"),
            expr("round(rand()*50, 2)").alias("jitter_ms"),
            expr("round(rand()*5, 2)").alias("packet_loss_percent"),
            expr("round(-1*(rand()*40 + 60), 2)").alias("signal_strength_dbm"),
            expr("round(rand()*100, 2)").alias("signal_quality_percent"),
            expr("round(rand()*100, 2)").alias("network_availability_percent"),
            expr("round(rand()*5 + 1, 2)").alias("call_setup_time_seconds"),
            expr("round(rand()*5, 2)").alias("call_drop_rate_percent"),
            expr("round(rand()*5 + 1, 2)").alias("sms_delivery_time_seconds"),
            expr("round(rand()*100, 2)").alias("sms_success_rate_percent"),
            expr("round(rand()*5 + 1, 2)").alias("data_session_setup_time_seconds"),
            expr("round(rand()*5 + 1, 2)").alias("website_loading_time_seconds"),
            expr(pick_array_str(["Poor","Fair","Good","Excellent"])) .alias("video_streaming_quality"),
            expr("cast(rand()*10 as int)").alias("video_buffering_events"),
            expr("round(rand()*5, 2)").alias("voice_quality_mos"),
            expr("round(rand()*100, 2)").alias("coverage_indoor_percent"),
            expr("round(rand()*100, 2)").alias("coverage_outdoor_percent"),
            expr("round(rand()*10 + 2, 2)").alias("price_per_gb"),
            expr("round(rand()*2 + 0.5, 2)").alias("voice_rate_per_minute"),
            expr("round(rand()*1 + 0.2, 2)").alias("sms_rate_per_message"),
            expr("concat('Bundle ', cast(cast(rand()*20 as int) as string))").alias("bundle_offerings"),
            expr("round(rand()*10, 2)").alias("customer_service_rating"),
            expr("round(rand()*10, 2)").alias("brand_perception_score"),
        )
    )
    write_single(comp, f"{RAW_BASE}/competitor_analysis_data")

    # ========== Regulatory Compliance Data (monthly-like sample) ==========
    reg = (
        towers_index_lookup.limit(_scale(5000))
        .select(
            col("tower_id"),
            random_timestamp_in_interval(start_time - timedelta(days=60), end_time).alias("reporting_period"),
            expr("concat('LIC', lpad(cast(cast(rand()*999999 as int) as string), 6, '0'))").alias("license_number"),
            expr("round(rand()*3000 + 700, 2)").alias("frequency_allocation_mhz"),
            expr("round(rand()*80 + 10, 2)").alias("transmit_power_watts"),
            expr("rand()<0.95").alias("coverage_obligation_met"),
            expr("round(rand()*100, 2)").alias("quality_of_service_score"),
            expr("round(rand()*100, 2)").alias("call_success_rate_percent"),
            expr("round(rand()*5, 2)").alias("call_drop_rate_percent"),
            expr("round(rand()*100, 2)").alias("network_availability_percent"),
            expr("cast(rand()*100 as int)").alias("customer_complaints_count"),
            expr("cast(rand()*100 as int)").alias("customer_complaints_resolved"),
            expr("round(rand()*1e6, 2)").alias("interconnection_charges"),
            expr("round(rand()*1e6, 2)").alias("universal_service_contribution"),
            expr("round(rand()*1e6, 2)").alias("spectrum_usage_fee"),
            expr("rand()<0.98").alias("environmental_compliance"),
            expr("rand()<0.99").alias("radiation_levels_compliant"),
            expr("rand()<0.97").alias("tower_marking_compliant"),
            expr("rand()<0.9").alias("backup_power_compliance"),
            expr("rand()<0.95").alias("security_requirements_met"),
            expr("rand()<0.9").alias("data_protection_compliant"),
            random_timestamp_in_interval(start_time - timedelta(days=180), end_time).alias("audit_date"),
            expr("concat('Findings ', cast(cast(rand()*100 as int) as string))").alias("audit_findings"),
            expr("concat('Actions ', cast(cast(rand()*100 as int) as string))").alias("corrective_actions_required"),
            random_timestamp_in_interval(end_time, end_time + timedelta(days=180)).alias("compliance_deadline"),
            expr("round(rand()*100000, 2)").alias("fine_amount"),
            random_timestamp_in_interval(end_time, end_time + timedelta(days=365)).alias("license_renewal_date"),
            expr("round(rand()*500, 2)").alias("geographic_coverage_km2"),
            expr("cast(rand()*100000 as int)").alias("population_covered"),
        )
    )
    write_single(reg, f"{RAW_BASE}/regulatory_compliance_data")

    # ========== Load Shedding Schedules ==========
    ls = (
        spark.range(_scale(1000))
        .select(
            random_timestamp_in_interval(start_time, end_time).alias("timestamp"),
            expr("concat('Municipality ', cast(cast(rand()*100 as int) as string))").alias("municipality"),
            expr("concat('Suburb ', cast(cast(rand()*1000 as int) as string))").alias("suburb"),
            expr("cast(rand()*9 as int)").alias("load_shedding_stage"),
            random_timestamp_in_interval(start_time, end_time).alias("scheduled_start_time"),
        )
        .withColumn("scheduled_end_time", expr("timestampadd(MINUTE, cast(rand()*240 as int), scheduled_start_time)"))
        .withColumn("actual_start_time", expr("case when rand()<0.8 then scheduled_start_time else timestampadd(MINUTE, cast(rand()*30 as int), scheduled_start_time) end"))
        .withColumn("actual_end_time", expr("timestampadd(MINUTE, cast(rand()*240 as int), actual_start_time)"))
        .withColumn("outage_duration_minutes", expr("cast((unix_timestamp(scheduled_end_time)-unix_timestamp(scheduled_start_time))/60 as int)"))
        .withColumn("block_number", expr("concat('B', lpad(cast(cast(rand()*999 as int) as string), 3, '0'))"))
        .withColumn("feeder_id", expr("concat('FDR', lpad(cast(cast(rand()*99999 as int) as string), 5, '0'))"))
        .withColumn("substation_name", expr("concat('Substation ', cast(cast(rand()*500 as int) as string))"))
        .withColumn("voltage_level", expr(pick_array_str(["11kV","22kV","132kV"])) )
        .withColumn("customer_count_affected", expr("cast(rand()*20000 as int)"))
        .withColumn("industrial_customers_affected", expr("cast(rand()*1000 as int)"))
        .withColumn("critical_infrastructure_affected", expr("rand()<0.05"))
        .withColumn("alternative_supply_available", expr("rand()<0.2"))
        .withColumn("emergency_exemption", expr("rand()<0.02"))
        .withColumn("weather_related", expr("rand()<0.1"))
        .withColumn("maintenance_related", expr("rand()<0.15"))
        .withColumn("equipment_failure", expr("rand()<0.12"))
        .withColumn("overload_related", expr("rand()<0.18"))
        .withColumn("estimated_restore_time", expr("timestampadd(MINUTE, cast(rand()*300 as int), scheduled_start_time)"))
        .withColumn("update_source", expr(pick_array_str(["Eskom","Municipality","Field"])) )
    )
    write_single(ls, f"{RAW_BASE}/load_shedding_schedules")

    # ========== Maintenance Crew (master; write only if missing) ==========
    crew_path = f"{RAW_BASE}/maintenance_crew"
    if _should_refresh_master("maintenance_crew") or not _exists(crew_path):
        crew = (
            spark.range(_scale(2000))
            .select(
                expr("concat('CREW', lpad(cast(id as string), 6, '0'))").alias("crew_id"),
                expr("concat('LEAD', lpad(cast(id as string), 6, '0'))").alias("crew_leader_id"),
                expr("cast(rand()*10 + 2 as int)").alias("crew_size"),
                expr(pick_array_str(["Network","Power","Structural","Emergency"])) .alias("specialization"),
                expr(pick_array_str(["Level1","Level2","Level3","Expert"])) .alias("certification_level"),
                expr(pick_array_str(["Gauteng","KwaZulu-Natal","Western Cape","Eastern Cape","Free State","Mpumalanga","Northern Cape","Limpopo","North West"])) .alias("region"),
                (rand() * 7 + 22).alias("base_location_lat"),
                (rand() * 9 + 16).alias("base_location_lng"),
                (rand() * 7 + 22).alias("current_location_lat"),
                (rand() * 9 + 16).alias("current_location_lng"),
                expr(pick_array_str(["Available","Busy","Off-duty","Emergency"])) .alias("availability_status"),
                random_timestamp_in_interval(start_time, end_time).alias("shift_start_time"),
            )
            .withColumn("shift_end_time", expr("timestampadd(HOUR, 8, shift_start_time)"))
            .withColumn("vehicle_id", expr("concat('VEH', lpad(cast(cast(rand()*9999 as int) as string), 4, '0'))"))
            .withColumn("equipment_inventory", expr("concat('Tools ', cast(cast(rand()*10 as int) as string))"))
            .withColumn("contact_number", expr("concat('+27-0', lpad(cast(cast(rand()*9999999 as int) as string), 7, '0'))"))
            .withColumn("emergency_contact", expr("concat('+27-0', lpad(cast(cast(rand()*9999999 as int) as string), 7, '0'))"))
            .withColumn("last_training_date", random_timestamp_in_interval(start_time - timedelta(days=365), end_time))
            .withColumn("safety_certification", expr("rand()<0.95"))
            .withColumn("years_experience", expr("cast(rand()*15 as int)"))
            .withColumn("performance_rating", expr("round(rand()*100, 2)"))
            .withColumn("languages_spoken", expr("concat('en, ', element_at(array('af','zu','xh','st','tn'), cast(rand()*5 as int)+1))"))
            .withColumn("overtime_hours_month", expr("cast(rand()*40 as int)"))
        )
        write_single(crew, crew_path)
    _rm_path(f"{RAW_BASE}/_control/maintenance_crew.refresh")

    # ========== Network Traffic Analytics (15-min) ==========
    quarters = spark.range(16).select(expr(f"timestampadd(MINUTE, -15 * cast(id as int), to_timestamp('{end_time.strftime('%Y-%m-%d %H:%M:%S')}'))").alias("timestamp")).filter(col("timestamp") >= to_timestamp(lit(start_time.strftime('%Y-%m-%d %H:%M:%S'))))
    some_towers = towers_index_lookup.limit(_scale(500)).select("tower_id")
    nta = (
        some_towers.crossJoin(quarters)
        .select(
            col("timestamp"),
            col("tower_id"),
            expr("concat('SEC', lpad(cast(cast(rand()*999 as int) as string), 3, '0'))").alias("sector_id"),
            expr(pick_array_str(["HTTP","HTTPS","FTP","SMTP","DNS"])) .alias("protocol_type"),
            expr(pick_array_str(["YouTube","Netflix","Facebook","WhatsApp","Teams"])) .alias("application_protocol"),
            expr(pick_array_str(["Inbound","Outbound","Internal"])) .alias("traffic_direction"),
            expr("cast(rand()*1e10 as bigint)").alias("total_bytes"),
            expr("cast(rand()*1e8 as bigint)").alias("total_packets"),
            expr("cast(rand()*10000 as int)").alias("unique_sessions"),
            expr("cast(rand()*5000 as int)").alias("peak_concurrent_sessions"),
            expr("round(rand()*1800, 2)").alias("average_session_duration_seconds"),
            expr("cast(rand()*100000 as int)").alias("tcp_connections"),
            expr("cast(rand()*80000 as int)").alias("udp_connections"),
            expr("cast(rand()*70000 as int)").alias("ssl_connections"),
            expr("cast(rand()*1e10 as bigint)").alias("ipv4_traffic_bytes"),
            expr("cast(rand()*1e9 as bigint)").alias("ipv6_traffic_bytes"),
            expr("cast(rand()*1e10 as bigint)").alias("domestic_traffic_bytes"),
            expr("cast(rand()*1e10 as bigint)").alias("international_traffic_bytes"),
            expr("cast(rand()*1e10 as bigint)").alias("cdn_traffic_bytes"),
            expr("cast(rand()*1e9 as bigint)").alias("p2p_traffic_bytes"),
            expr("cast(rand()*1e10 as bigint)").alias("streaming_traffic_bytes"),
            expr("cast(rand()*1e9 as bigint)").alias("gaming_traffic_bytes"),
            expr("cast(rand()*1e9 as bigint)").alias("social_media_traffic_bytes"),
            expr("cast(rand()*1e9 as bigint)").alias("email_traffic_bytes"),
            expr("cast(rand()*1e10 as bigint)").alias("web_browsing_traffic_bytes"),
            expr("cast(rand()*1e9 as bigint)").alias("voip_traffic_bytes"),
            expr("cast(rand()*1e9 as bigint)").alias("video_call_traffic_bytes"),
            expr("cast(rand()*1000 as int)").alias("malware_detected_packets"),
            expr("cast(rand()*5000 as int)").alias("blocked_connections"),
        )
    )
    write_single(nta, f"{RAW_BASE}/network_traffic_analytics")

    # ========== Financial and Billing Data (daily batch; generate small slice) ==========
    billing = (
        spark.range(_scale(20000))
        .select(
            random_timestamp_in_interval(start_time - timedelta(days=1), end_time).alias("transaction_date"),
            expr("concat('ACC', lpad(cast(id as string), 8, '0'))").alias("account_id"),
            expr("concat('CUST', lpad(cast(id as string), 8, '0'))").alias("anonymized_customer_id"),
            expr(pick_array_str(["Voice","Data","SMS","Bundle"])) .alias("service_type"),
            expr(pick_array_str(["Basic","Plus","Pro","Enterprise"])) .alias("tariff_plan"),
            expr("round(rand()*200, 2)").alias("usage_charges"),
            expr("round(rand()*500, 2)").alias("monthly_charges"),
            expr("round(rand()*50, 2)").alias("roaming_charges"),
            expr("round(rand()*50, 2)").alias("international_charges"),
            expr("round(rand()*100, 2)").alias("premium_service_charges"),
            expr("round(rand()*100, 2)").alias("taxes"),
            expr("round(rand()*1000, 2)").alias("total_amount"),
            expr(pick_array_str(["Credit","Debit","EFT","Cash","Voucher"])) .alias("payment_method"),
            expr(pick_array_str(["Paid","Pending","Overdue","Failed"])) .alias("payment_status"),
            expr(pick_array_str(["ZAR","USD","EUR"])) .alias("currency"),
            expr(pick_array_str(["Monthly","Prepaid","Pay-as-you-go"])) .alias("billing_cycle"),
            expr(pick_array_str(["Month-to-month","12Month","24Month"])) .alias("contract_type"),
            expr(pick_array_str(["Consumer","SME","Enterprise","Government"])) .alias("customer_segment"),
            expr(pick_array_str(["Active","Suspended","Cancelled"])) .alias("account_status"),
            expr("round(rand()*5000, 2)").alias("credit_limit"),
            expr("round(rand()*5000, 2)").alias("current_balance"),
            expr("round(rand()*3000, 2)").alias("outstanding_amount"),
            random_timestamp_in_interval(start_time - timedelta(days=30), end_time).alias("last_payment_date"),
            expr("round(rand()*1000, 2)").alias("last_payment_amount"),
            expr("rand()<0.3").alias("auto_payment_enabled"),
            expr("rand()<0.4").alias("loyalty_program_member"),
            expr("round(rand()*200, 2)").alias("promotional_discounts"),
        )
    )
    write_single(billing, f"{RAW_BASE}/financial_billing_data")

    # ========== Network Maintenance Records ==========
    maint = (
        spark.range(_scale(2000))
        .select(
            expr("concat('MNT', lpad(cast(id as string), 8, '0'))").alias("maintenance_id"),
            random_timestamp_in_interval(start_time, end_time).alias("timestamp"),
        )
        .withColumn("technician_id", expr("concat('TECH', lpad(cast(cast(rand()*999999 as int) as string), 6, '0'))"))
        .withColumn("technician_team_size", expr("cast(rand()*5 + 1 as int)"))
        .withColumn("contractor_company", expr(pick_array_str(["Contractor A","Contractor B","Contractor C"])) )
        .withColumn("work_order_number", expr("concat('WO', lpad(cast(cast(rand()*999999 as int) as string), 6, '0'))"))
        .withColumn("priority_level", expr(pick_array_str(["Low","Medium","High","Emergency"])) )
        .withColumn("service_impact", expr(pick_array_str(["None","Partial","Full","Extended"])) )
        .withColumn("estimated_customers_affected", expr("cast(rand()*5000 as int)"))
        .withColumn("maintenance_category", expr(pick_array_str(["Software","Hardware","Infrastructure","Power"])) )
        .withColumn("parts_replaced", expr("concat('Part-', cast(cast(rand()*100 as int) as string))"))
        .withColumn("parts_cost", expr("round(rand()*5000, 2)"))
        .withColumn("labor_hours", expr("round(rand()*16, 2)"))
        .withColumn("labor_cost", expr("round(labor_hours*500, 2)"))
        .withColumn("travel_cost", expr("round(rand()*1000, 2)"))
        .withColumn("equipment_downtime_minutes", expr("cast(rand()*600 as int)"))
        .withColumn("pre_maintenance_tests", expr("'basic checks'"))
        .withColumn("post_maintenance_tests", expr("'post checks'"))
        .withColumn("maintenance_success", expr("rand()<0.9"))
        .withColumn("follow_up_required", expr("rand()<0.1"))
        .withColumn("warranty_work", expr("rand()<0.2"))
        .withColumn("safety_incidents", expr("cast(rand()*2 as int)"))
        .withColumn("weather_conditions", expr(pick_array_str(["Clear","Rain","Windy","Storm"])) )
        .withColumn("access_issues", expr(pick_array_str(["None","Road","Security"])) )
        .withColumn("completion_notes", expr("concat('Completed ', cast(id as string))"))
        .withColumn("maintenance_type", expr(pick_array_str(["Preventive","Corrective","Emergency","Upgrade"])) )
        .withColumn("scheduled_start_time", random_timestamp_in_interval(start_time - timedelta(hours=4), end_time))
        .withColumn("scheduled_end_time", expr("timestampadd(HOUR, 2, scheduled_start_time)"))
        .withColumn("actual_start_time", col("scheduled_start_time"))
        .withColumn("actual_end_time", col("scheduled_end_time"))
    )
    # Attach tower/equipment
    maint = maint.withColumn("tower_index", pmod(spark_hash(col("maintenance_id")), lit(total_towers)).cast("int")).join(towers_index_lookup, on="tower_index", how="left").drop("tower_index")
    maint = maint.withColumn("equipment_id", expr("concat('EQ-', lpad(cast(cast(rand()*99999999 as int) as string), 8, '0'))"))
    write_single(maint.select("maintenance_id","timestamp","tower_id","equipment_id","maintenance_type","scheduled_start_time","scheduled_end_time","actual_start_time","actual_end_time","technician_id","technician_team_size","contractor_company","work_order_number","priority_level","service_impact","estimated_customers_affected","maintenance_category","parts_replaced","parts_cost","labor_hours","labor_cost","travel_cost","equipment_downtime_minutes","pre_maintenance_tests","post_maintenance_tests","maintenance_success","follow_up_required","warranty_work","safety_incidents","weather_conditions","access_issues","completion_notes"), f"{RAW_BASE}/network_maintenance_records")

    # ========== Tower Connectivity ==========
    # Build simple ring neighbors like historical
    neighbor_map = towers_index_lookup.select(col("tower_id").alias("dst_tower_id"), col("tower_index"))
    src_with_idx = towers_index_lookup.select(col("tower_id").alias("src_tower_id"), col("tower_index"))
    total = total_towers
    edges_idx = src_with_idx.select(col("src_tower_id"), expr(f"(tower_index + 1) % {total}").alias("neighbor_index")).union(
        src_with_idx.select(col("src_tower_id"), expr(f"(tower_index + 2) % {total}").alias("neighbor_index"))
    )
    connectivity = (
        edges_idx.join(neighbor_map.select(col("dst_tower_id"), col("tower_index").alias("neighbor_index")), on="neighbor_index", how="left")
        .select(
            "src_tower_id","dst_tower_id",
            expr(pick_array_str(["Fiber","Microwave","Satellite"])) .alias("connection_type"),
            expr("cast(rand()*1000 + 100 as int)").alias("link_capacity_mbps"),
            expr("round(rand()*50 + 1, 2)").alias("link_distance_km"),
            expr("round(-1*(rand()*30 + 50), 2)").alias("signal_quality_dbm"),
            random_timestamp_in_interval(start_time, end_time).alias("timestamp"),
            expr("round(rand()*50, 2)").alias("latency_ms"),
            expr("round(rand()*5, 2)").alias("packet_loss_percent"),
            expr("round(rand()*100, 2)").alias("availability_percent"),
            expr("round(rand()*10, 2)").alias("bandwidth_cost_per_mbps"),
            random_timestamp_in_interval(start_time - timedelta(days=365), end_time).alias("installation_date"),
            random_timestamp_in_interval(start_time - timedelta(days=30), end_time).alias("last_maintenance_date"),
            expr(pick_array_str(["VendorA","VendorB","VendorC"])) .alias("vendor"),
            expr("concat('Model ', cast(cast(rand()*50 as int) as string))").alias("equipment_model"),
            expr("rand()<0.3").alias("backup_link_available"),
            expr(pick_array_str(["Critical","High","Medium","Low"])) .alias("priority_level"),
            expr("rand()<0.8").alias("encryption_enabled"),
            expr("rand()<0.95").alias("monitoring_enabled"),
        )
    )
    write_single(connectivity, f"{RAW_BASE}/tower_connectivity")

    # ========== Tower Capacity (hourly) ==========
    cap = (
        towers_index_lookup.select(col("tower_id"))
        .join(hours)
        .select(
            col("tower_id"),
            col("timestamp"),
            expr("cast(rand()*2000 + 500 as int)").alias("total_capacity_mbps"),
            expr("cast(rand()*1500 + 100 as int)").alias("current_utilization_mbps"),
            expr("cast(rand()*1500 + 100 as int)").alias("peak_utilization_mbps"),
            expr("cast(rand()*1000 + 100 as int)").alias("available_capacity_mbps"),
            expr("cast(rand()*6 + 3 as int)").alias("sector_count"),
            expr("cast(rand()*200 as int)").alias("technology_2g_capacity"),
            expr("cast(rand()*300 as int)").alias("technology_3g_capacity"),
            expr("cast(rand()*800 as int)").alias("technology_4g_capacity"),
            expr("cast(rand()*1000 as int)").alias("technology_5g_capacity"),
            expr("cast(rand()*500 as int)").alias("voice_capacity_erlangs"),
            expr("cast(rand()*1500 as int)").alias("data_capacity_mbps"),
            expr("cast(rand()*2000 as int)").alias("backhaul_capacity_mbps"),
            expr("cast(rand()*5000 as int)").alias("power_consumption_watts"),
            expr("cast(rand()*50000 as int)").alias("cooling_capacity_btuh"),
            expr("round(rand()*100, 2)").alias("equipment_rack_utilization"),
            expr(pick_array_str(["Low","Medium","High"])) .alias("cable_management_capacity"),
            expr("cast(rand()*500 as int)").alias("emergency_capacity_mbps"),
            expr("cast(rand()*1000 as int)").alias("upgrade_potential_mbps"),
            expr("concat('Note ', cast(cast(rand()*9999 as int) as string))").alias("capacity_planning_notes"),
        )
    )
    write_single(cap, f"{RAW_BASE}/tower_capacity")

    # ========== Tower Imagery (daily) ==========
    imagery = (
        towers_index_lookup.select("tower_id")
        .limit(_scale(15000))
        .select(
            col("tower_id"),
            random_timestamp_in_interval(start_time - timedelta(hours=12), end_time).alias("timestamp"),
            expr("concat('abfss://raw@dlstelkomnetworkprod.dfs.core.windows.net/assets/tower_images/', tower_id, '/', cast(unix_timestamp() as string), '.jpg')").alias("image_path"),
            expr("concat('DRN', lpad(cast(cast(rand()*9999 as int) as string), 4, '0'))").alias("drone_id"),
            expr("concat('PIL', lpad(cast(cast(rand()*9999 as int) as string), 4, '0'))").alias("pilot_id"),
            expr(pick_array_str(["Clear","Cloudy","Windy","Rainy"])) .alias("weather_conditions"),
            expr(pick_array_str(["4K","8K"])) .alias("image_resolution"),
            expr(pick_array_str(["Front","Side","Top","360"])) .alias("camera_angle"),
            expr("cast(rand()*120 + 20 as int)").alias("flight_altitude_meters"),
            expr("concat('(', cast(round(rand()*7 + 22, 5) as string), ',', cast(round(rand()*9 + 16, 5) as string), ')')").alias("gps_coordinates"),
            expr(pick_array_str(["Daylight","Dusk","Dawn","Artificial"])) .alias("lighting_conditions"),
            expr("round(rand()*100, 2)").alias("image_quality_score"),
            expr("round(rand()*50, 2)").alias("file_size_mb"),
            expr(pick_array_str(["Manual","Automated","Scheduled"])) .alias("capture_mode"),
            expr(pick_array_str(["Routine","Emergency","Post-maintenance"])) .alias("inspection_type"),
        )
    )
    write_single(imagery, f"{RAW_BASE}/tower_imagery")

    return True

# === Main Execution ===
if __name__ == "__main__":
    generate_interval_data()

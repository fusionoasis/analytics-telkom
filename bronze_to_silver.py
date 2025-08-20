from pyspark.sql.functions import (
    udf,
    to_utc_timestamp,
    when,
    sha2,
    concat_ws,
    coalesce,
    lit,
    expr,
    col,
    date_format,
    hour,
    upper,
    trim,
    round as spark_round,
)
from pyspark.sql.types import (
    DoubleType,
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    TimestampType,
)
from delta.tables import DeltaTable

try:
    spark
except NameError:
    spark = None

def dew_point_c(temp_c, humidity):
    import math
    if temp_c is None or humidity is None or humidity <= 0:
        return None
    a, b = 17.62, 243.12
    try:
        gamma = (a * temp_c / (b + temp_c)) + math.log(humidity / 100.0)
        return (b * gamma) / (a - gamma)
    except Exception:
        return None

udf_dew = udf(dew_point_c, DoubleType())


class TelkomBronzeToSilver:
    def __init__(self, base_path="/mnt/dlstelkomnetworkprod"):
        self.base_path = base_path
        self.bronze_path = f"{base_path}/bronze"
        self.silver_path = f"{base_path}/silver"
        self.catalog = "telkom_analytics"
        self.timezone = "Africa/Johannesburg"
        if spark is None:
            raise RuntimeError("Databricks global 'spark' session not available. Run this on Databricks.")
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog} LOCATION '{self.silver_path}'")
        spark.sql(f"USE {self.catalog}")

    def to_utc(self, col_ts):
        return to_utc_timestamp(col_ts, self.timezone)

    def clamp(self, col, lo, hi):
        return when(col < lo, lo).when(col > hi, hi).otherwise(col)

    def sha256_str(self, *cols):
        return sha2(concat_ws("||", *[coalesce(c.cast("string"), lit("")) for c in cols]), 256)

    def safe_round(self, col, scale):
        return spark_round(col.cast("double"), scale)

    # ---------- Incremental load helpers ----------
    def ensure_watermark_table(self):
        """Create the watermark tracking table if it doesn't exist."""
        meta_path = f"{self.silver_path}/_meta/etl_watermarks"
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.etl_watermarks (
              table_name STRING,
              watermark_ts TIMESTAMP
            ) USING DELTA LOCATION '{meta_path}'
            """
        )

    def get_watermark(self, table_name: str):
        """Return the last processed watermark timestamp for a table, or None."""
        try:
            df = spark.table(f"{self.catalog}.etl_watermarks").where(col("table_name") == lit(table_name)).select(
                "watermark_ts"
            ).orderBy(col("watermark_ts").desc()).limit(1)
            rows = df.collect()
            return rows[0][0] if rows else None
        except Exception:
            return None

    def update_watermark(self, table_name: str, watermark_ts):
        """Upsert the watermark for a table."""
        src = spark.createDataFrame([(table_name, watermark_ts)], ["table_name", "watermark_ts"])
        tgt = DeltaTable.forName(spark, f"{self.catalog}.etl_watermarks")
        (
            tgt.alias("t").merge(src.alias("s"), "t.table_name = s.table_name")
            .whenMatchedUpdate(set={"watermark_ts": col("s.watermark_ts")})
            .whenNotMatchedInsert(values={"table_name": col("s.table_name"), "watermark_ts": col("s.watermark_ts")})
            .execute()
        )

    def has_rows(self, df) -> bool:
        """Return True if DataFrame has at least one row."""
        return df.limit(1).count() > 0

    def compute_and_update_watermark(self, df, ts_col: str, table_name: str):
        """Compute max(ts_col) from df and update watermark for table_name."""
        if not self.has_rows(df):
            return
        max_row = df.agg({ts_col: "max"}).collect()
        wm = max_row[0][0] if max_row and max_row[0] else None
        if wm is not None:
            self.update_watermark(table_name, wm)

    def merge_into_delta_table(self, source_df, target_table, partition_cols, merge_condition, update_cols, insert_cols):
        target_path = f"{self.silver_path}/{target_table}"
        if not self.has_rows(source_df):
            print(f"No new data for {target_table}; skipping merge.")
            return
        delta_table = (
            DeltaTable.forPath(spark, target_path)
            if spark.catalog.tableExists(f"{self.catalog}.{target_table}")
            else None
        )
        if delta_table:
            update_map = {k: expr(v) if isinstance(v, str) else v for k, v in update_cols.items()}
            insert_map = {k: expr(v) if isinstance(v, str) else v for k, v in insert_cols.items()}
            (
                delta_table.alias("target")
                .merge(source_df.alias("source"), merge_condition)
                .whenMatchedUpdate(set=update_map)
                .whenNotMatchedInsert(values=insert_map)
                .execute()
            )
        else:
            writer = source_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            writer.save(target_path)
            spark.sql(
                f"CREATE TABLE IF NOT EXISTS {self.catalog}.{target_table} USING DELTA LOCATION '{target_path}'"
            )
        spark.sql(
            f"OPTIMIZE {self.catalog}.{target_table} ZORDER BY ({', '.join(source_df.columns[:2])})"
        )
        spark.sql(f"ANALYZE TABLE {self.catalog}.{target_table} COMPUTE STATISTICS")

    def create_silver_tables(self):
        print("Creating silver tables from bronze...")

        REGIONS = [
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
        region_df = spark.createDataFrame(
            [(i, r) for i, r in enumerate(REGIONS, start=1)], ["region_key", "region"]
        )
        region_df.write.format("delta").mode("overwrite").saveAsTable(
            f"{self.catalog}.dim_region_silver"
        )

        # tower_locations
        tower_schema = StructType(
            [
                StructField("tower_id", StringType(), True),
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True),
                StructField("region_index", IntegerType(), True),
                StructField("region", StringType(), True),
            ]
        )
        towers_raw = spark.read.schema(tower_schema).parquet(
            f"{self.bronze_path}/tower_locations"
        )
        towers_silver = (
            towers_raw.dropDuplicates(["tower_id"])
            .withColumn("latitude", self.clamp(col("latitude"), -35.0, -22.0))
            .withColumn("longitude", self.clamp(col("longitude"), 16.0, 33.0))
            .withColumn("latitude", self.safe_round(col("latitude"), 6))
            .withColumn("longitude", self.safe_round(col("longitude"), 6))
            .withColumn("region", upper(trim(col("region"))))
            .join(region_df, "region", "inner")
            .withColumn("tower_sk", self.sha256_str(col("tower_id")))
            .select("tower_sk", "tower_id", "region_key", "region", "latitude", "longitude")
        )
        self.merge_into_delta_table(
            source_df=towers_silver,
            target_table="tower_locations",
            partition_cols=["region_key"],
            merge_condition="target.tower_sk = source.tower_sk",
            update_cols={
                "tower_id": "source.tower_id",
                "region_key": "source.region_key",
                "region": "source.region",
                "latitude": "source.latitude",
                "longitude": "source.longitude",
            },
            insert_cols={
                "tower_sk": "source.tower_sk",
                "tower_id": "source.tower_id",
                "region_key": "source.region_key",
                "region": "source.region",
                "latitude": "source.latitude",
                "longitude": "source.longitude",
            },
        )

        # network_logs
        nw_schema = StructType(
            [
                StructField("tower_id", StringType(), True),
                StructField("signal_strength", DoubleType(), True),
                StructField("latency_ms", DoubleType(), True),
                StructField("timestamp", TimestampType(), True),
                StructField("uptime", DoubleType(), True),
                StructField("error_codes", StringType(), True),
                StructField("year", IntegerType(), True),
                StructField("month", IntegerType(), True),
                StructField("day", IntegerType(), True),
                StructField("region_index", IntegerType(), True),
                StructField("region", StringType(), True),
            ]
        )
        nw_raw = spark.read.schema(nw_schema).parquet(
            f"{self.bronze_path}/network_logs"
        )
        nw_wm = self.get_watermark("network_logs")
        nw_enriched = (
            nw_raw.withColumn("ts_utc", self.to_utc(col("timestamp")))
            .withColumn("region", upper(trim(col("region"))))
            .join(region_df, "region", "inner")
            .join(towers_silver.select("tower_id", "tower_sk"), "tower_id", "inner")
            .withColumn("signal_strength", self.clamp(col("signal_strength"), 0.0, 100.0))
            .withColumn("latency_ms", self.clamp(col("latency_ms"), 0.0, 1000.0))
            .withColumn("uptime", self.clamp(col("uptime"), 0.0, 100.0))
            .withColumn("error_code", coalesce(col("error_codes"), lit("NONE")))
            .withColumn(
                "performance_category",
                when((col("signal_strength") > 80) & (col("latency_ms") < 50), "GOOD").otherwise("POOR"),
            )
        )
        if nw_wm is not None:
            nw_enriched = nw_enriched.filter(col("ts_utc") > lit(nw_wm))
        nw_silver = (
            nw_enriched.withColumn("date_key", date_format(col("ts_utc"), "yyyyMMdd").cast("int"))
            .withColumn("hour_key", hour(col("ts_utc")))
            .withColumn("nw_row_sk", self.sha256_str(col("tower_sk"), col("ts_utc")))
            .dropDuplicates(["nw_row_sk"]).select(
                "nw_row_sk",
                "tower_sk",
                "region_key",
                "date_key",
                "ts_utc",
                "signal_strength",
                "latency_ms",
                "uptime",
                "error_code",
                "performance_category",
                "hour_key",
            )
        )
        self.merge_into_delta_table(
            source_df=nw_silver,
            target_table="network_logs",
            partition_cols=["date_key"],
            merge_condition="target.nw_row_sk = source.nw_row_sk",
            update_cols={
                "tower_sk": "source.tower_sk",
                "region_key": "source.region_key",
                "date_key": "source.date_key",
                "ts_utc": "source.ts_utc",
                "signal_strength": "source.signal_strength",
                "latency_ms": "source.latency_ms",
                "uptime": "source.uptime",
                "error_code": "source.error_code",
                "performance_category": "source.performance_category",
                "hour_key": "source.hour_key",
            },
            insert_cols={
                "nw_row_sk": "source.nw_row_sk",
                "tower_sk": "source.tower_sk",
                "region_key": "source.region_key",
                "date_key": "source.date_key",
                "ts_utc": "source.ts_utc",
                "signal_strength": "source.signal_strength",
                "latency_ms": "source.latency_ms",
                "uptime": "source.uptime",
                "error_code": "source.error_code",
                "performance_category": "source.performance_category",
                "hour_key": "source.hour_key",
            },
        )
        self.compute_and_update_watermark(nw_silver, "ts_utc", "network_logs")

        # weather_data
        w_schema = StructType(
            [
                StructField("tower_id", StringType(), True),
                StructField("timestamp", TimestampType(), True),
                StructField("temperature_c", DoubleType(), True),
                StructField("humidity_percent", DoubleType(), True),
                StructField("wind_speed_mps", DoubleType(), True),
                StructField("weather_condition", StringType(), True),
                StructField("visibility_km", DoubleType(), True),
                StructField("year", IntegerType(), True),
                StructField("month", IntegerType(), True),
                StructField("day", IntegerType(), True),
                StructField("region_index", IntegerType(), True),
                StructField("region", StringType(), True),
            ]
        )
        weather_raw = spark.read.schema(w_schema).parquet(
            f"{self.bronze_path}/weather_data"
        )
        weather_wm = self.get_watermark("weather_data")
        weather_enriched = (
            weather_raw.withColumn("ts_utc", self.to_utc(col("timestamp")))
            .withColumn("region", upper(trim(col("region"))))
            .join(region_df, "region", "inner")
            .join(towers_silver.select("tower_id", "tower_sk"), "tower_id", "inner")
            .withColumn("temperature_c", self.clamp(col("temperature_c"), -40.0, 60.0))
            .withColumn("humidity_percent", self.clamp(col("humidity_percent"), 0.0, 100.0))
            .withColumn("wind_speed_mps", self.clamp(col("wind_speed_mps"), 0.0, 50.0))
            .withColumn("visibility_km", self.clamp(col("visibility_km"), 0.0, 100.0))
            .withColumn("weather_condition", upper(trim(col("weather_condition"))))
            .withColumn("dew_point_c", udf_dew(col("temperature_c"), col("humidity_percent")))
            .withColumn(
                "weather_severity",
                when(col("weather_condition") == "RAIN", 3)
                .when(col("weather_condition") == "CLOUDS", 2)
                .otherwise(1),
            )
        )
        if weather_wm is not None:
            weather_enriched = weather_enriched.filter(col("ts_utc") > lit(weather_wm))
        weather_silver = (
            weather_enriched.withColumn("date_key", date_format(col("ts_utc"), "yyyyMMdd").cast("int"))
            .withColumn("hour_key", hour(col("ts_utc")))
            .withColumn("weather_row_sk", self.sha256_str(col("tower_sk"), col("ts_utc")))
            .dropDuplicates(["weather_row_sk"]).select(
                "weather_row_sk",
                "tower_sk",
                "region_key",
                "date_key",
                "ts_utc",
                "temperature_c",
                "humidity_percent",
                "wind_speed_mps",
                "visibility_km",
                "weather_condition",
                "dew_point_c",
                "weather_severity",
                "hour_key",
            )
        )
        self.merge_into_delta_table(
            source_df=weather_silver,
            target_table="weather_data",
            partition_cols=["date_key"],
            merge_condition="target.weather_row_sk = source.weather_row_sk",
            update_cols={
                "tower_sk": "source.tower_sk",
                "region_key": "source.region_key",
                "date_key": "source.date_key",
                "ts_utc": "source.ts_utc",
                "temperature_c": "source.temperature_c",
                "humidity_percent": "source.humidity_percent",
                "wind_speed_mps": "source.wind_speed_mps",
                "visibility_km": "source.visibility_km",
                "weather_condition": "source.weather_condition",
                "dew_point_c": "source.dew_point_c",
                "weather_severity": "source.weather_severity",
                "hour_key": "source.hour_key",
            },
            insert_cols={
                "weather_row_sk": "source.weather_row_sk",
                "tower_sk": "source.tower_sk",
                "region_key": "source.region_key",
                "date_key": "source.date_key",
                "ts_utc": "source.ts_utc",
                "temperature_c": "source.temperature_c",
                "humidity_percent": "source.humidity_percent",
                "wind_speed_mps": "source.wind_speed_mps",
                "visibility_km": "source.visibility_km",
                "weather_condition": "source.weather_condition",
                "dew_point_c": "source.dew_point_c",
                "weather_severity": "source.weather_severity",
                "hour_key": "source.hour_key",
            },
        )
        self.compute_and_update_watermark(weather_silver, "ts_utc", "weather_data")

        # customer_usage
        cu_schema = StructType(
            [
                StructField("customer_id", StringType(), True),
                StructField("data_usage_mb", DoubleType(), True),
                StructField("call_duration_min", DoubleType(), True),
                StructField("timestamp", TimestampType(), True),
                StructField("data_usage", DoubleType(), True),
                StructField("call_duration", DoubleType(), True),
                StructField("year", IntegerType(), True),
                StructField("month", IntegerType(), True),
                StructField("day", IntegerType(), True),
            ]
        )
        cu_raw = spark.read.schema(cu_schema).parquet(
            f"{self.bronze_path}/customer_usage"
        )
        cu_wm = self.get_watermark("customer_usage")
        cu_enriched = (
            cu_raw.withColumn("ts_utc", self.to_utc(col("timestamp")))
            .withColumn("data_usage_mb", self.clamp(col("data_usage_mb"), 0.0, 50000.0))
            .withColumn("call_duration_min", self.clamp(col("call_duration_min"), 0.0, 1440.0))
            .withColumn(
                "usage_category",
                when((col("data_usage_mb") > 1000) | (col("call_duration_min") > 60), "HIGH").otherwise("LOW"),
            )
        )
        if cu_wm is not None:
            cu_enriched = cu_enriched.filter(col("ts_utc") > lit(cu_wm))
        cu_silver = (
            cu_enriched.withColumn("date_key", date_format(col("ts_utc"), "yyyyMMdd").cast("int"))
            .withColumn("hour_key", hour(col("ts_utc")))
            .withColumn("customer_sk", self.sha256_str(col("customer_id")))
            .withColumn("usage_row_sk", self.sha256_str(col("customer_sk"), col("ts_utc")))
            .dropDuplicates(["usage_row_sk"]).select(
                "usage_row_sk",
                "customer_sk",
                "date_key",
                "ts_utc",
                "data_usage_mb",
                "call_duration_min",
                "usage_category",
                "hour_key",
            )
        )
        self.merge_into_delta_table(
            source_df=cu_silver,
            target_table="customer_usage",
            partition_cols=["date_key"],
            merge_condition="target.usage_row_sk = source.usage_row_sk",
            update_cols={
                "customer_sk": "source.customer_sk",
                "date_key": "source.date_key",
                "ts_utc": "source.ts_utc",
                "data_usage_mb": "source.data_usage_mb",
                "call_duration_min": "source.call_duration_min",
                "usage_category": "source.usage_category",
                "hour_key": "source.hour_key",
            },
            insert_cols={
                "usage_row_sk": "source.usage_row_sk",
                "customer_sk": "source.customer_sk",
                "date_key": "source.date_key",
                "ts_utc": "source.ts_utc",
                "data_usage_mb": "source.data_usage_mb",
                "call_duration_min": "source.call_duration_min",
                "usage_category": "source.usage_category",
                "hour_key": "source.hour_key",
            },
        )
        self.compute_and_update_watermark(cu_silver, "ts_utc", "customer_usage")

        # load_shedding_schedules
        ls_schema = StructType(
            [
                StructField("region", StringType(), True),
                StructField("start_time", TimestampType(), True),
                StructField("end_time", TimestampType(), True),
                StructField("year", IntegerType(), True),
                StructField("month", IntegerType(), True),
                StructField("day", IntegerType(), True),
            ]
        )
        ls_raw = spark.read.schema(ls_schema).parquet(
            f"{self.bronze_path}/load_shedding_schedules"
        )
        ls_wm = self.get_watermark("load_shedding_schedules")
        ls_enriched = (
            ls_raw.withColumn("region", upper(trim(col("region"))))
            .join(region_df, "region", "inner")
            .withColumn("start_utc", self.to_utc(col("start_time")))
            .withColumn("end_utc", self.to_utc(col("end_time")))
            .filter(col("end_utc") > col("start_utc"))
        )
        if ls_wm is not None:
            ls_enriched = ls_enriched.filter(col("start_utc") > lit(ls_wm))
        ls_silver = (
            ls_enriched.withColumn(
                "duration_hours", (col("end_utc").cast("long") - col("start_utc").cast("long")) / 3600.0
            )
            .withColumn("date_key", date_format(col("start_utc"), "yyyyMMdd").cast("int"))
            .withColumn("hour_key", hour(col("start_utc")))
            .withColumn("ls_row_sk", self.sha256_str(col("region_key"), col("start_utc"), col("end_utc")))
            .dropDuplicates(["ls_row_sk"]).select(
                "ls_row_sk",
                "region_key",
                "date_key",
                "start_utc",
                "end_utc",
                "duration_hours",
                "hour_key",
            )
        )
        self.merge_into_delta_table(
            source_df=ls_silver,
            target_table="load_shedding_schedules",
            partition_cols=["date_key"],
            merge_condition="target.ls_row_sk = source.ls_row_sk",
            update_cols={
                "region_key": "source.region_key",
                "date_key": "source.date_key",
                "start_utc": "source.start_utc",
                "end_utc": "source.end_utc",
                "duration_hours": "source.duration_hours",
                "hour_key": "source.hour_key",
            },
            insert_cols={
                "ls_row_sk": "source.ls_row_sk",
                "region_key": "source.region_key",
                "date_key": "source.date_key",
                "start_utc": "source.start_utc",
                "end_utc": "source.end_utc",
                "duration_hours": "source.duration_hours",
                "hour_key": "source.hour_key",
            },
        )
        self.compute_and_update_watermark(ls_silver, "start_utc", "load_shedding_schedules")

        # customer_feedback
        cf_schema = StructType(
            [
                StructField("text", StringType(), True),
                StructField("sentiment_score", DoubleType(), True),
                StructField("timestamp", TimestampType(), True),
                StructField("sentiment_label", StringType(), True),
                StructField("year", IntegerType(), True),
                StructField("month", IntegerType(), True),
                StructField("day", IntegerType(), True),
            ]
        )
        cf_raw = spark.read.schema(cf_schema).parquet(
            f"{self.bronze_path}/customer_feedback"
        )
        cf_wm = self.get_watermark("customer_feedback")
        cf_enriched = (
            cf_raw.withColumn("ts_utc", self.to_utc(col("timestamp")))
            .withColumn("sentiment_score", self.clamp(col("sentiment_score"), -1.0, 1.0))
            .withColumn("sentiment_label", upper(trim(col("sentiment_label"))))
        )
        if cf_wm is not None:
            cf_enriched = cf_enriched.filter(col("ts_utc") > lit(cf_wm))
        cf_silver = (
            cf_enriched.withColumn("date_key", date_format(col("ts_utc"), "yyyyMMdd").cast("int"))
            .withColumn("hour_key", hour(col("ts_utc")))
            .withColumn("feedback_row_sk", self.sha256_str(col("text"), col("ts_utc")))
            .dropDuplicates(["feedback_row_sk"]).select(
                "feedback_row_sk",
                "date_key",
                "ts_utc",
                "text",
                "sentiment_score",
                "sentiment_label",
                "hour_key",
            )
        )
        self.merge_into_delta_table(
            source_df=cf_silver,
            target_table="customer_feedback",
            partition_cols=["date_key"],
            merge_condition="target.feedback_row_sk = source.feedback_row_sk",
            update_cols={
                "date_key": "source.date_key",
                "ts_utc": "source.ts_utc",
                "text": "source.text",
                "sentiment_score": "source.sentiment_score",
                "sentiment_label": "source.sentiment_label",
                "hour_key": "source.hour_key",
            },
            insert_cols={
                "feedback_row_sk": "source.feedback_row_sk",
                "date_key": "source.date_key",
                "ts_utc": "source.ts_utc",
                "text": "source.text",
                "sentiment_score": "source.sentiment_score",
                "sentiment_label": "source.sentiment_label",
                "hour_key": "source.hour_key",
            },
        )
        self.compute_and_update_watermark(cf_silver, "ts_utc", "customer_feedback")

        # tower_imagery
        ti_schema = StructType(
            [
                StructField("tower_id", StringType(), True),
                StructField("timestamp", TimestampType(), True),
                StructField("image_path", StringType(), True),
                StructField("condition_label", StringType(), True),
                StructField("year", IntegerType(), True),
                StructField("month", IntegerType(), True),
                StructField("day", IntegerType(), True),
            ]
        )
        ti_raw = spark.read.schema(ti_schema).parquet(
            f"{self.bronze_path}/tower_imagery"
        )
        ti_wm = self.get_watermark("tower_imagery")
        ti_enriched = (
            ti_raw.withColumn("ts_utc", self.to_utc(col("timestamp")))
            .join(towers_silver.select("tower_id", "tower_sk"), "tower_id", "inner")
            .withColumn("condition_label", upper(trim(col("condition_label"))))
        )
        if ti_wm is not None:
            ti_enriched = ti_enriched.filter(col("ts_utc") > lit(ti_wm))
        ti_silver = (
            ti_enriched.withColumn("date_key", date_format(col("ts_utc"), "yyyyMMdd").cast("int"))
            .withColumn("hour_key", hour(col("ts_utc")))
            .withColumn("imagery_row_sk", self.sha256_str(col("tower_sk"), col("ts_utc"), col("image_path")))
            .dropDuplicates(["imagery_row_sk"]).select(
                "imagery_row_sk",
                "tower_sk",
                "date_key",
                "ts_utc",
                "image_path",
                "condition_label",
                "hour_key",
            )
        )
        self.merge_into_delta_table(
            source_df=ti_silver,
            target_table="tower_imagery",
            partition_cols=["date_key"],
            merge_condition="target.imagery_row_sk = source.imagery_row_sk",
            update_cols={
                "tower_sk": "source.tower_sk",
                "date_key": "source.date_key",
                "ts_utc": "source.ts_utc",
                "image_path": "source.image_path",
                "condition_label": "source.condition_label",
                "hour_key": "source.hour_key",
            },
            insert_cols={
                "imagery_row_sk": "source.imagery_row_sk",
                "tower_sk": "source.tower_sk",
                "date_key": "source.date_key",
                "ts_utc": "source.ts_utc",
                "image_path": "source.image_path",
                "condition_label": "source.condition_label",
                "hour_key": "source.hour_key",
            },
        )
        self.compute_and_update_watermark(ti_silver, "ts_utc", "tower_imagery")

        # voice_transcriptions
        vt_schema = StructType(
            [
                StructField("technician_id", StringType(), True),
                StructField("transcription", StringType(), True),
                StructField("timestamp", TimestampType(), True),
                StructField("year", IntegerType(), True),
                StructField("month", IntegerType(), True),
                StructField("day", IntegerType(), True),
            ]
        )
        vt_raw = spark.read.schema(vt_schema).parquet(
            f"{self.bronze_path}/voice_transcriptions"
        )
        vt_wm = self.get_watermark("voice_transcriptions")
        vt_enriched = (
            vt_raw.withColumn("ts_utc", self.to_utc(col("timestamp")))
            .withColumn("transcription", upper(trim(col("transcription"))))
            .withColumn("technician_sk", self.sha256_str(col("technician_id")))
        )
        if vt_wm is not None:
            vt_enriched = vt_enriched.filter(col("ts_utc") > lit(vt_wm))
        vt_silver = (
            vt_enriched.withColumn("date_key", date_format(col("ts_utc"), "yyyyMMdd").cast("int"))
            .withColumn("hour_key", hour(col("ts_utc")))
            .withColumn(
                "transcription_row_sk",
                self.sha256_str(col("technician_sk"), col("ts_utc"), col("transcription")),
            )
            .dropDuplicates(["transcription_row_sk"]).select(
                "transcription_row_sk",
                "technician_sk",
                "date_key",
                "ts_utc",
                "transcription",
                "hour_key",
            )
        )
        self.merge_into_delta_table(
            source_df=vt_silver,
            target_table="voice_transcriptions",
            partition_cols=["date_key"],
            merge_condition="target.transcription_row_sk = source.transcription_row_sk",
            update_cols={
                "technician_sk": "source.technician_sk",
                "date_key": "source.date_key",
                "ts_utc": "source.ts_utc",
                "transcription": "source.transcription",
                "hour_key": "source.hour_key",
            },
            insert_cols={
                "transcription_row_sk": "source.transcription_row_sk",
                "technician_sk": "source.technician_sk",
                "date_key": "source.date_key",
                "ts_utc": "source.ts_utc",
                "transcription": "source.transcription",
                "hour_key": "source.hour_key",
            },
        )
        self.compute_and_update_watermark(vt_silver, "ts_utc", "voice_transcriptions")

        # tower_connectivity
        tc_schema = StructType(
            [
                StructField("src_tower_id", StringType(), True),
                StructField("dst_tower_id", StringType(), True),
                StructField("signal_quality", DoubleType(), True),
                StructField("timestamp", TimestampType(), True),
                StructField("year", IntegerType(), True),
                StructField("month", IntegerType(), True),
                StructField("day", IntegerType(), True),
            ]
        )
        tc_raw = spark.read.schema(tc_schema).parquet(
            f"{self.bronze_path}/tower_connectivity"
        )
        tc_wm = self.get_watermark("tower_connectivity")
        tc_enriched = (
            tc_raw.withColumn("ts_utc", self.to_utc(col("timestamp")))
            .join(
                towers_silver.select("tower_id", "tower_sk").withColumnRenamed("tower_sk", "src_tower_sk"),
                col("src_tower_id") == col("tower_id"),
                "inner",
            )
            .join(
                towers_silver.select("tower_id", "tower_sk").withColumnRenamed("tower_sk", "dst_tower_sk"),
                col("dst_tower_id") == col("tower_id"),
                "inner",
            )
            .withColumn("signal_quality", self.clamp(col("signal_quality"), 0.0, 100.0))
        )
        if tc_wm is not None:
            tc_enriched = tc_enriched.filter(col("ts_utc") > lit(tc_wm))
        tc_silver = (
            tc_enriched.withColumn("date_key", date_format(col("ts_utc"), "yyyyMMdd").cast("int"))
            .withColumn("hour_key", hour(col("ts_utc")))
            .withColumn("connectivity_row_sk", self.sha256_str(col("src_tower_sk"), col("dst_tower_sk"), col("ts_utc")))
            .dropDuplicates(["connectivity_row_sk"]).select(
                "connectivity_row_sk",
                "src_tower_sk",
                "dst_tower_sk",
                "date_key",
                "ts_utc",
                "signal_quality",
                "hour_key",
            )
        )
        self.merge_into_delta_table(
            source_df=tc_silver,
            target_table="tower_connectivity",
            partition_cols=["date_key"],
            merge_condition="target.connectivity_row_sk = source.connectivity_row_sk",
            update_cols={
                "src_tower_sk": "source.src_tower_sk",
                "dst_tower_sk": "source.dst_tower_sk",
                "date_key": "source.date_key",
                "ts_utc": "source.ts_utc",
                "signal_quality": "source.signal_quality",
                "hour_key": "source.hour_key",
            },
            insert_cols={
                "connectivity_row_sk": "source.connectivity_row_sk",
                "src_tower_sk": "source.src_tower_sk",
                "dst_tower_sk": "source.dst_tower_sk",
                "date_key": "source.date_key",
                "ts_utc": "source.ts_utc",
                "signal_quality": "source.signal_quality",
                "hour_key": "source.hour_key",
            },
        )
        self.compute_and_update_watermark(tc_silver, "ts_utc", "tower_connectivity")

        # tower_capacity
        tcap_schema = StructType(
            [
                StructField("tower_id", StringType(), True),
                StructField("capacity_mbps", DoubleType(), True),
                StructField("utilization_percent", DoubleType(), True),
                StructField("timestamp", TimestampType(), True),
                StructField("year", IntegerType(), True),
                StructField("month", IntegerType(), True),
                StructField("day", IntegerType(), True),
            ]
        )
        tcap_raw = spark.read.schema(tcap_schema).parquet(
            f"{self.bronze_path}/tower_capacity"
        )
        tcap_wm = self.get_watermark("tower_capacity")
        tcap_enriched = (
            tcap_raw.withColumn("ts_utc", self.to_utc(col("timestamp")))
            .join(towers_silver.select("tower_id", "tower_sk"), "tower_id", "inner")
            .withColumn("capacity_mbps", self.clamp(col("capacity_mbps"), 0.0, 10000.0))
            .withColumn("utilization_percent", self.clamp(col("utilization_percent"), 0.0, 100.0))
        )
        if tcap_wm is not None:
            tcap_enriched = tcap_enriched.filter(col("ts_utc") > lit(tcap_wm))
        tcap_silver = (
            tcap_enriched.withColumn("date_key", date_format(col("ts_utc"), "yyyyMMdd").cast("int"))
            .withColumn("hour_key", hour(col("ts_utc")))
            .withColumn("capacity_row_sk", self.sha256_str(col("tower_sk"), col("ts_utc")))
            .dropDuplicates(["capacity_row_sk"]).select(
                "capacity_row_sk",
                "tower_sk",
                "date_key",
                "ts_utc",
                "capacity_mbps",
                "utilization_percent",
                "hour_key",
            )
        )
        self.merge_into_delta_table(
            source_df=tcap_silver,
            target_table="tower_capacity",
            partition_cols=["date_key"],
            merge_condition="target.capacity_row_sk = source.capacity_row_sk",
            update_cols={
                "tower_sk": "source.tower_sk",
                "date_key": "source.date_key",
                "ts_utc": "source.ts_utc",
                "capacity_mbps": "source.capacity_mbps",
                "utilization_percent": "source.utilization_percent",
                "hour_key": "source.hour_key",
            },
            insert_cols={
                "capacity_row_sk": "source.capacity_row_sk",
                "tower_sk": "source.tower_sk",
                "date_key": "source.date_key",
                "ts_utc": "source.ts_utc",
                "capacity_mbps": "source.capacity_mbps",
                "utilization_percent": "source.utilization_percent",
                "hour_key": "source.hour_key",
            },
        )
        self.compute_and_update_watermark(tcap_silver, "ts_utc", "tower_capacity")

        # maintenance_crew
        mc_schema = StructType(
            [
                StructField("crew_id", StringType(), True),
                StructField("region", StringType(), True),
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True),
                StructField("available", BooleanType(), True),
                StructField("shift_start", TimestampType(), True),
                StructField("shift_end", TimestampType(), True),
                StructField("year", IntegerType(), True),
                StructField("month", IntegerType(), True),
                StructField("day", IntegerType(), True),
            ]
        )
        mc_raw = spark.read.schema(mc_schema).parquet(
            f"{self.bronze_path}/maintenance_crew"
        )
        mc_wm = self.get_watermark("maintenance_crew")
        mc_enriched = (
            mc_raw.withColumn("region", upper(trim(col("region"))))
            .join(region_df, "region", "inner")
            .withColumn("shift_start_utc", self.to_utc(col("shift_start")))
            .withColumn("shift_end_utc", self.to_utc(col("shift_end")))
            .withColumn("latitude", self.clamp(col("latitude"), -35.0, -22.0))
            .withColumn("longitude", self.clamp(col("longitude"), 16.0, 33.0))
            .withColumn("latitude", self.safe_round(col("latitude"), 6))
            .withColumn("longitude", self.safe_round(col("longitude"), 6))
        )
        if mc_wm is not None:
            mc_enriched = mc_enriched.filter(col("shift_start_utc") > lit(mc_wm))
        mc_silver = (
            mc_enriched.withColumn("date_key", date_format(col("shift_start_utc"), "yyyyMMdd").cast("int"))
            .withColumn("crew_sk", self.sha256_str(col("crew_id")))
            .withColumn("crew_row_sk", self.sha256_str(col("crew_sk"), col("shift_start_utc")))
            .dropDuplicates(["crew_row_sk"]).select(
                "crew_row_sk",
                "crew_sk",
                "region_key",
                "date_key",
                "shift_start_utc",
                "shift_end_utc",
                "latitude",
                "longitude",
                "available",
            )
        )
        self.merge_into_delta_table(
            source_df=mc_silver,
            target_table="maintenance_crew",
            partition_cols=["date_key"],
            merge_condition="target.crew_row_sk = source.crew_row_sk",
            update_cols={
                "crew_sk": "source.crew_sk",
                "region_key": "source.region_key",
                "date_key": "source.date_key",
                "shift_start_utc": "source.shift_start_utc",
                "shift_end_utc": "source.shift_end_utc",
                "latitude": "source.latitude",
                "longitude": "source.longitude",
                "available": "source.available",
            },
            insert_cols={
                "crew_row_sk": "source.crew_row_sk",
                "crew_sk": "source.crew_sk",
                "region_key": "source.region_key",
                "date_key": "source.date_key",
                "shift_start_utc": "source.shift_start_utc",
                "shift_end_utc": "source.shift_end_utc",
                "latitude": "source.latitude",
                "longitude": "source.longitude",
                "available": "source.available",
            },
        )
        self.compute_and_update_watermark(mc_silver, "shift_start_utc", "maintenance_crew")

    def generate_silver_summary(self):
        print("Generating silver layer summary...")
        tables_to_check = [
            "dim_region_silver",
            "tower_locations",
            "network_logs",
            "weather_data",
            "customer_usage",
            "load_shedding_schedules",
            "customer_feedback",
            "tower_imagery",
            "voice_transcriptions",
            "tower_connectivity",
            "tower_capacity",
            "maintenance_crew",
        ]
        table_stats = {}
        for table in tables_to_check:
            try:
                count = spark.table(f"{self.catalog}.{table}").count()
                table_stats[table] = count
                print(f"📊 {table}: {count:,} rows")
            except Exception as e:
                print(f"⚠️ Could not get count for {table}: {e}")
        print("🏗️ Silver Layer Summary:")
        print("   📊 12 tables created with partitioning by date_key or region_key")
        print("   🔄 Incremental updates via MERGE operations")
        print("   ✅ Data cleaned and enriched with surrogate keys")
        print("   🚀 Optimized with ZORDER for query performance")

    def run_bronze_to_silver_pipeline(self):
        print("Starting Bronze to Silver Pipeline...")
        try:
            self.ensure_watermark_table()
            self.create_silver_tables()
            print("✅ Silver tables created successfully")
            self.generate_silver_summary()
            print("🎉 Bronze to Silver Pipeline completed successfully!")
        except Exception as e:
            print(f"❌ Pipeline failed: {str(e)}")
            raise


if spark is not None:
    pipeline = TelkomBronzeToSilver()
    pipeline.run_bronze_to_silver_pipeline()
else:
    print("Databricks global 'spark' not detected; skipping pipeline run.")
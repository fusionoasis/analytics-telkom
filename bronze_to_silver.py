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
    def __init__(self, base_path=None, bronze_external_location: str = "bronze-layer", silver_base_path=None, silver_external_location: str = "silver-layer"):
        # Normalize base path to include a cloud/DBFS scheme for SQL compatibility
        def _normalize_base(p: str) -> str:
            if p.startswith(("dbfs:/", "abfss://", "s3://", "wasbs://")):
                return p
            if p.startswith("/"):
                return f"dbfs:{p}"
            return p

        self.bronze_external_location = bronze_external_location
        self.silver_external_location = silver_external_location

        # Prefer Unity Catalog semantics when available; fall back silently if not
        if spark is None:
            raise RuntimeError("Databricks global 'spark' session not available. Run this on Databricks.")

        # Set Spark session timezone
        # Store all timestamps as UTC; convert inputs from Africa/Johannesburg to UTC downstream.
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        # Allow schema evolution during MERGE operations when adding new columns (e.g., tower_sk)
        try:
            spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        except Exception:
            pass

        # Resolve bronze and silver roots independently. Prefer UC External Locations; allow explicit overrides.
        def _resolve_external_location(loc_name: str):
            try:
                if not loc_name:
                    return None
                df = spark.sql(f"DESCRIBE EXTERNAL LOCATION `{loc_name}`")
                if "url" in df.columns:
                    row = df.limit(1).collect()[0]
                    return str(row["url"]).rstrip("/")
                # Older style key/value rows
                for r in df.collect():
                    for k in ("url", "URL"):
                        if hasattr(r, k):
                            return str(getattr(r, k)).rstrip("/")
            except Exception:
                return None

        # Bronze root
        bronze_root = None
        if base_path:
            bronze_root = _normalize_base(str(base_path).rstrip("/"))
        else:
            bronze_root = _resolve_external_location(self.bronze_external_location)
        if not bronze_root:
            # Final fallback for non-UC local mounts
            bronze_root = _normalize_base("/mnt/dlstelkomnetworkprod/bronze")

        # Silver root
        silver_root = None
        if silver_base_path:
            silver_root = _normalize_base(str(silver_base_path).rstrip("/"))
        else:
            silver_root = _resolve_external_location(self.silver_external_location)
        if not silver_root:
            # Final fallback for non-UC local mounts
            silver_root = _normalize_base("/mnt/dlstelkomnetworkprod/silver")

        # Persist paths. Keep base_path for backward compatibility but set it to bronze_root.
        self.base_path = bronze_root
        self.bronze_path = bronze_root
        self.silver_path = silver_root
        self.catalog = "telkom_analytics"
        self.timezone = "Africa/Johannesburg"

    # Create silver base directory if possible (works on Databricks)
        try:
            from pyspark.dbutils import DBUtils  # type: ignore
            # Only attempt mkdirs for DBFS paths
            if str(self.silver_path).startswith("dbfs:/"):
                DBUtils(spark).fs.mkdirs(self.silver_path)
        except Exception:
            pass

        try:
            spark.sql("USE CATALOG main")
        except Exception:
            pass
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}")
        spark.sql(f"USE SCHEMA {self.catalog}")



    def to_utc(self, col_ts):
        return to_utc_timestamp(col_ts, self.timezone)

    def clamp(self, col, lo, hi):
        return when(col < lo, lo).when(col > hi, hi).otherwise(col)

    def sha256_str(self, *cols):
        return sha2(concat_ws("||", *[coalesce(c.cast("string"), lit("")) for c in cols]), 256)

    def safe_round(self, col, scale):
        return spark_round(col.cast("double"), scale)

    # ---------- Incremental load helpers ----------
    def check_storage(self) -> bool:
        """Fast check that bronze storage is reachable; prints a helpful message on failure."""
        try:
            from pyspark.dbutils import DBUtils  # type: ignore
            DBUtils(spark).fs.ls(self.bronze_path)
            return True
        except Exception as e:
            # If using a cloud URI, some orgs keep data under a nested '/bronze' folder beneath the container root.
            # Try that alternative path once before failing.
            try:
                if not str(self.base_path).startswith("dbfs:/") and not str(self.bronze_path).rstrip("/").endswith("/bronze"):
                    candidate = f"{self.base_path}/bronze"
                    DBUtils(spark).fs.ls(candidate)
                    print(f"ℹ️ Switching bronze path to nested folder: {candidate}")
                    self.bronze_path = candidate
                    return True
            except Exception:
                pass
            print(f"⚠️ Storage check failed for {self.bronze_path}: {e}")
            print(
                "If you're using Unity Catalog external locations, ensure the compute has permissions and the URL is under the external location bound to your storage credential."
            )
            try:
                # Show configured external locations if available
                if getattr(self, "bronze_external_location", None):
                    desc = spark.sql(f"DESCRIBE EXTERNAL LOCATION `{self.bronze_external_location}`")
                    desc.show(truncate=False)
                if getattr(self, "silver_external_location", None):
                    desc = spark.sql(f"DESCRIBE EXTERNAL LOCATION `{self.silver_external_location}`")
                    desc.show(truncate=False)
            except Exception:
                pass
            return False
    def ensure_watermark_table(self):
        """Create the watermark tracking table if it doesn't exist."""
        meta_path = f"{self.silver_path}/_meta/etl_watermarks"
        # Use managed table when only DBFS path is available; LOCATION requires cloud scheme.
        if str(self.silver_path).startswith("dbfs:/"):
            spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {self.catalog}.etl_watermarks (
                    table_name STRING,
                    watermark_ts TIMESTAMP
                ) USING DELTA
                """
            )
        else:
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

    def merge_into_delta_table(self, source_df, target_table, partition_cols, merge_condition, update_cols, insert_cols, zorder_cols=None):
        target_path = f"{self.silver_path}/{target_table}"
        has_data = self.has_rows(source_df)
        # Use table name for Delta operations
        delta_table = None
        exists = False
        # Standard existence check; handle stale UC entries pointing to missing/invalid paths by auto-dropping
        try:
            exists = spark.catalog.tableExists(f"{self.catalog}.{target_table}")
        except Exception as e:
            msg = str(e)
            if "DELTA_PATH_DOES_NOT_EXIST" in msg or "42K03" in msg:
                print(
                    f"ℹ️ Detected stale catalog entry for {self.catalog}.{target_table} with missing path. Dropping and recreating under silver."
                )
                spark.sql(f"DROP TABLE IF EXISTS {self.catalog}.{target_table}")
                exists = False
            else:
                raise
        # If there is no new data, ensure the table exists (create empty skeleton if missing) and return
        if not has_data:
            if exists:
                print(f"No new data for {target_table}; table already exists; skipping merge.")
                return
            # Create an empty Delta table with the expected schema
            empty_df = source_df.limit(0)
            writer = empty_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            if str(self.silver_path).startswith("dbfs:/"):
                writer.saveAsTable(f"{self.catalog}.{target_table}")
            else:
                writer.save(target_path)
                spark.sql(
                    f"CREATE TABLE IF NOT EXISTS {self.catalog}.{target_table} USING DELTA LOCATION '{target_path}'"
                )
            print(f"No new data for {target_table}; created empty Delta table at {target_path}.")
            return
        if exists:
            try:
                delta_table = DeltaTable.forName(spark, f"{self.catalog}.{target_table}")
            except Exception as e:
                msg = str(e)
                if "DELTA_PATH_DOES_NOT_EXIST" in msg or "is not a Delta table" in msg:
                    print(
                        f"ℹ️ Existing table {self.catalog}.{target_table} points to an invalid/missing path. Dropping and recreating under silver."
                    )
                    spark.sql(f"DROP TABLE IF EXISTS {self.catalog}.{target_table}")
                    delta_table = None
                else:
                    raise RuntimeError(
                        f"Table {self.catalog}.{target_table} exists but could not be opened as a Delta table: {e}"
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
            # If using DBFS silver path, create a managed table (no LOCATION). Otherwise, register external path.
            if str(self.silver_path).startswith("dbfs:/"):
                writer.saveAsTable(f"{self.catalog}.{target_table}")
            else:
                writer.save(target_path)
                spark.sql(
                    f"CREATE TABLE IF NOT EXISTS {self.catalog}.{target_table} USING DELTA LOCATION '{target_path}'"
                )
        # Explicit Z-ORDER columns improve query performance; avoid partition columns.
        # Start with provided zorder_cols or fall back to the first source column.
        proposed = zorder_cols if zorder_cols else [source_df.columns[0]]
        # Remove any partition columns and duplicates while preserving order.
        part_set = set(partition_cols or [])
        seen = set()
        zcols = []
        for c in proposed:
            if c in part_set or c in seen:
                continue
            seen.add(c)
            zcols.append(c)
        # Run OPTIMIZE ZORDER only if we have at least one non-partition column.
        if zcols:
            spark.sql(
                f"OPTIMIZE {self.catalog}.{target_table} ZORDER BY ({', '.join(zcols)})"
            )
        else:
            print(f"ℹ️ Skipping Z-ORDER for {target_table} because only partition columns were provided.")
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
        ).withColumn("region", upper(col("region")))
        # Ensure the dimension table is stored under the silver root as well
        if str(self.silver_path).startswith("dbfs:/"):
            region_df.write.format("delta").mode("overwrite").saveAsTable(
                f"{self.catalog}.dim_region_silver"
            )
        else:
            dim_path = f"{self.silver_path}/dim_region_silver"
            region_df.write.format("delta").mode("overwrite").save(dim_path)
            # Recreate as an external table at the silver location to guarantee placement
            spark.sql(f"DROP TABLE IF EXISTS {self.catalog}.dim_region_silver")
            spark.sql(
                f"CREATE TABLE {self.catalog}.dim_region_silver USING DELTA LOCATION '{dim_path}'"
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
            zorder_cols=["region_key", "tower_sk"],
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
        try:
            nw_exists = spark.catalog.tableExists(f"{self.catalog}.network_logs")
        except Exception:
            nw_exists = False
        nw_wm = self.get_watermark("network_logs") if nw_exists else None
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
            zorder_cols=["tower_sk", "ts_utc"],
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
        try:
            weather_exists = spark.catalog.tableExists(f"{self.catalog}.weather_data")
        except Exception:
            weather_exists = False
        weather_wm = self.get_watermark("weather_data") if weather_exists else None
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
            zorder_cols=["tower_sk", "ts_utc"],
        )
        self.compute_and_update_watermark(weather_silver, "ts_utc", "weather_data")

        # customer_usage
        cu_schema = StructType(
            [
                StructField("customer_id", StringType(), True),
                StructField("tower_id", StringType(), True),
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
        try:
            cu_exists = spark.catalog.tableExists(f"{self.catalog}.customer_usage")
        except Exception:
            cu_exists = False
        cu_wm = self.get_watermark("customer_usage") if cu_exists else None
        cu_enriched = (
            cu_raw.withColumn("ts_utc", self.to_utc(col("timestamp")))
            .join(towers_silver.select("tower_id", "tower_sk"), "tower_id", "left")
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
                "tower_sk",
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
                "tower_sk": "source.tower_sk",
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
                "tower_sk": "source.tower_sk",
                "date_key": "source.date_key",
                "ts_utc": "source.ts_utc",
                "data_usage_mb": "source.data_usage_mb",
                "call_duration_min": "source.call_duration_min",
                "usage_category": "source.usage_category",
                "hour_key": "source.hour_key",
            },
            zorder_cols=["tower_sk", "customer_sk", "ts_utc"],
        )
        self.compute_and_update_watermark(cu_silver, "ts_utc", "customer_usage")

        # load_shedding_schedules
        ls_schema = StructType(
            [
                StructField("region", StringType(), True),
                StructField("tower_id", StringType(), True),
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
        try:
            ls_exists = spark.catalog.tableExists(f"{self.catalog}.load_shedding_schedules")
        except Exception:
            ls_exists = False
        ls_wm = self.get_watermark("load_shedding_schedules") if ls_exists else None
        ls_enriched = (
            ls_raw.withColumn("region", upper(trim(col("region"))))
            .join(region_df, "region", "inner")
            .withColumn("start_utc", self.to_utc(col("start_time")))
            .withColumn("end_utc", self.to_utc(col("end_time")))
            .filter(col("end_utc") > col("start_utc"))
            .join(towers_silver.select("tower_id", "tower_sk"), "tower_id", "left")
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
                "tower_sk",
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
                "tower_sk": "source.tower_sk",
                "date_key": "source.date_key",
                "start_utc": "source.start_utc",
                "end_utc": "source.end_utc",
                "duration_hours": "source.duration_hours",
                "hour_key": "source.hour_key",
            },
            insert_cols={
                "ls_row_sk": "source.ls_row_sk",
                "region_key": "source.region_key",
                "tower_sk": "source.tower_sk",
                "date_key": "source.date_key",
                "start_utc": "source.start_utc",
                "end_utc": "source.end_utc",
                "duration_hours": "source.duration_hours",
                "hour_key": "source.hour_key",
            },
            zorder_cols=["tower_sk", "region_key", "start_utc"],
        )
        self.compute_and_update_watermark(ls_silver, "start_utc", "load_shedding_schedules")

        # customer_feedback
        cf_schema = StructType(
            [
                StructField("tower_id", StringType(), True),
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
        try:
            cf_exists = spark.catalog.tableExists(f"{self.catalog}.customer_feedback")
        except Exception:
            cf_exists = False
        cf_wm = self.get_watermark("customer_feedback") if cf_exists else None
        cf_enriched = (
            cf_raw.withColumn("ts_utc", self.to_utc(col("timestamp")))
            .join(towers_silver.select("tower_id", "tower_sk"), "tower_id", "left")
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
                "tower_sk",
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
                "tower_sk": "source.tower_sk",
                "date_key": "source.date_key",
                "ts_utc": "source.ts_utc",
                "text": "source.text",
                "sentiment_score": "source.sentiment_score",
                "sentiment_label": "source.sentiment_label",
                "hour_key": "source.hour_key",
            },
            insert_cols={
                "feedback_row_sk": "source.feedback_row_sk",
                "tower_sk": "source.tower_sk",
                "date_key": "source.date_key",
                "ts_utc": "source.ts_utc",
                "text": "source.text",
                "sentiment_score": "source.sentiment_score",
                "sentiment_label": "source.sentiment_label",
                "hour_key": "source.hour_key",
            },
            zorder_cols=["tower_sk", "ts_utc"],
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
        try:
            ti_exists = spark.catalog.tableExists(f"{self.catalog}.tower_imagery")
        except Exception:
            ti_exists = False
        ti_wm = self.get_watermark("tower_imagery") if ti_exists else None
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
            zorder_cols=["tower_sk", "ts_utc"],
        )
        self.compute_and_update_watermark(ti_silver, "ts_utc", "tower_imagery")

        # voice_transcriptions
        vt_schema = StructType(
            [
                StructField("technician_id", StringType(), True),
                StructField("tower_id", StringType(), True),
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
        try:
            vt_exists = spark.catalog.tableExists(f"{self.catalog}.voice_transcriptions")
        except Exception:
            vt_exists = False
        vt_wm = self.get_watermark("voice_transcriptions") if vt_exists else None
        vt_enriched = (
            vt_raw.withColumn("ts_utc", self.to_utc(col("timestamp")))
            .withColumn("transcription", upper(trim(col("transcription"))))
            .withColumn("technician_sk", self.sha256_str(col("technician_id")))
            .join(towers_silver.select("tower_id", "tower_sk"), "tower_id", "left")
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
                "tower_sk",
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
                "tower_sk": "source.tower_sk",
                "date_key": "source.date_key",
                "ts_utc": "source.ts_utc",
                "transcription": "source.transcription",
                "hour_key": "source.hour_key",
            },
            insert_cols={
                "transcription_row_sk": "source.transcription_row_sk",
                "technician_sk": "source.technician_sk",
                "tower_sk": "source.tower_sk",
                "date_key": "source.date_key",
                "ts_utc": "source.ts_utc",
                "transcription": "source.transcription",
                "hour_key": "source.hour_key",
            },
            zorder_cols=["technician_sk", "tower_sk", "ts_utc"],
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
        try:
            tc_exists = spark.catalog.tableExists(f"{self.catalog}.tower_connectivity")
        except Exception:
            tc_exists = False
        tc_wm = self.get_watermark("tower_connectivity") if tc_exists else None
        # Avoid ambiguous `tower_id` by pre-aliasing join keys and using explicit conditions
        src_map = towers_silver.select(
            col("tower_id").alias("src_join_tower_id"),
            col("tower_sk").alias("src_tower_sk"),
        )
        dst_map = towers_silver.select(
            col("tower_id").alias("dst_join_tower_id"),
            col("tower_sk").alias("dst_tower_sk"),
        )
        tc_enriched = (
            tc_raw.withColumn("ts_utc", self.to_utc(col("timestamp")))
            .join(src_map, tc_raw["src_tower_id"] == col("src_join_tower_id"), "inner")
            .join(dst_map, tc_raw["dst_tower_id"] == col("dst_join_tower_id"), "inner")
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
            zorder_cols=["src_tower_sk", "dst_tower_sk", "ts_utc"],
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
        try:
            tcap_exists = spark.catalog.tableExists(f"{self.catalog}.tower_capacity")
        except Exception:
            tcap_exists = False
        tcap_wm = self.get_watermark("tower_capacity") if tcap_exists else None
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
            zorder_cols=["tower_sk", "ts_utc"],
        )
        self.compute_and_update_watermark(tcap_silver, "ts_utc", "tower_capacity")

        # maintenance_crew
        mc_schema = StructType(
            [
                StructField("crew_id", StringType(), True),
                StructField("region", StringType(), True),
                StructField("tower_id", StringType(), True),
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
        try:
            mc_exists = spark.catalog.tableExists(f"{self.catalog}.maintenance_crew")
        except Exception:
            mc_exists = False
        mc_wm = self.get_watermark("maintenance_crew") if mc_exists else None
        mc_enriched = (
            mc_raw.withColumn("region", upper(trim(col("region"))))
            .join(region_df, "region", "inner")
            .withColumn("shift_start_utc", self.to_utc(col("shift_start")))
            .withColumn("shift_end_utc", self.to_utc(col("shift_end")))
            .withColumn("latitude", self.clamp(col("latitude"), -35.0, -22.0))
            .withColumn("longitude", self.clamp(col("longitude"), 16.0, 33.0))
            .withColumn("latitude", self.safe_round(col("latitude"), 6))
            .withColumn("longitude", self.safe_round(col("longitude"), 6))
            .join(towers_silver.select("tower_id", "tower_sk"), "tower_id", "left")
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
                "tower_sk",
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
                "tower_sk": "source.tower_sk",
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
                "tower_sk": "source.tower_sk",
                "date_key": "source.date_key",
                "shift_start_utc": "source.shift_start_utc",
                "shift_end_utc": "source.shift_end_utc",
                "latitude": "source.latitude",
                "longitude": "source.longitude",
                "available": "source.available",
            },
            zorder_cols=["tower_sk", "crew_sk", "shift_start_utc"],
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
            if not self.check_storage():
                raise RuntimeError(
                    "Cannot access bronze path. Verify Storage Credential/External Location and permissions, or update base_path."
                )
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
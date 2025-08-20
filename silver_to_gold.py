from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime
from typing import List, Optional

# Use the built-in Databricks `spark` session if present
try:
    spark  # type: ignore[name-defined]
except NameError:
    spark = None  # type: ignore[assignment]

class TelkomSilverToGold:
    def __init__(self, base_path="/mnt/dlstelkomnetworkprod"):
        self.base_path = base_path
        self.silver_path = f"{base_path}/silver"
        self.gold_path = f"{base_path}/gold"
        self.catalog = "telkom_analytics"
        # flags detected during setup
        self.dim_tower_has_identity = True
        self.delta_automerge = None
        self.delta_cdf_default = None
        if spark is None:
            print("spark session not found. This module expects to run on Databricks with a global `spark`.")
            return
        # Create database (with LOCATION if supported)
        try:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog} LOCATION '{self.gold_path}'")
        except Exception as e:
            print(f"Warning: could not set database location to {self.gold_path}: {e}. Using default location.")
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog}")
        spark.sql(f"USE {self.catalog}")
        # Enable schema auto-merge for MERGE operations that add columns
        try:
            spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
            self.delta_automerge = "true"
        except Exception:
            pass

    def check_delta_capabilities(self):
        """Log Delta-related capabilities from session conf for visibility."""
        try:
            self.delta_automerge = spark.conf.get("spark.databricks.delta.schema.autoMerge.enabled")
        except Exception:
            self.delta_automerge = "unknown"
        try:
            self.delta_cdf_default = spark.conf.get("spark.databricks.delta.properties.defaults.enableChangeDataFeed")
        except Exception:
            self.delta_cdf_default = "unknown"
        try:
            spark_extensions = spark.conf.get("spark.sql.extensions")
        except Exception:
            spark_extensions = "unknown"
        print(f"Delta capabilities: autoMerge={self.delta_automerge}, defaultCDF={self.delta_cdf_default}, extensions={spark_extensions}")

    # ---------- helpers ----------
    def _table_exists(self, name: str) -> bool:
        return bool(spark.catalog.tableExists(f"{self.catalog}.{name}"))

    def _optimize_table(self, table: str, zorder_cols: Optional[List[str]] = None):
        try:
            if zorder_cols:
                spark.sql(f"OPTIMIZE {self.catalog}.{table} ZORDER BY ({', '.join(zorder_cols)})")
            else:
                spark.sql(f"OPTIMIZE {self.catalog}.{table}")
            spark.sql(f"ANALYZE TABLE {self.catalog}.{table} COMPUTE STATISTICS")
        except Exception as e:
            print(f"OPTIMIZE/ANALYZE skipped for {self.catalog}.{table}: {e}")

    def _validate_columns(self, table: str, required_cols: List[str]) -> bool:
        try:
            df = spark.table(f"{self.catalog}.{table}")
            cols = set(df.columns)
            missing = [c for c in required_cols if c not in cols]
            if missing:
                print(f"Table {self.catalog}.{table} is missing columns: {missing}")
                return False
            return True
        except Exception as e:
            print(f"Could not read table {self.catalog}.{table} for schema validation: {e}")
            return False

    def merge_into_delta_table(self, source_df, target_table, partition_cols, merge_condition, update_cols, insert_cols):
        target_path = f"{self.gold_path}/{target_table}"
        delta_table = DeltaTable.forPath(spark, target_path) if spark.catalog.tableExists(f"{self.catalog}.{target_table}") else None
        if delta_table:
            delta_table.alias("target").merge(
                source_df.alias("source"),
                merge_condition
            ).whenMatchedUpdate(set=update_cols) \
             .whenNotMatchedInsert(values=insert_cols) \
             .execute()
        else:
            source_df.write \
                .format("delta") \
                .mode("overwrite") \
                .partitionBy(*partition_cols) \
                .option("overwriteSchema", "true") \
                .saveAsTable(f"{self.catalog}.{target_table}")
        self._optimize_table(target_table, source_df.columns[:2])

    def create_dimension_tables(self):
        print("Creating dimension tables…")

        # dim_region
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.dim_region (
            region_key INT,
            region STRING
        ) USING DELTA
        LOCATION '{self.gold_path}/dim_region'
        """)
        if self._table_exists("dim_region_silver") and self._validate_columns("dim_region_silver", ["region_key", "region"]):
            region_df = spark.read.table(f"{self.catalog}.dim_region_silver")
            region_df.createOrReplaceTempView("stg_dim_region")
            spark.sql(f"""
            MERGE INTO {self.catalog}.dim_region t
            USING (SELECT DISTINCT region_key, region FROM stg_dim_region) s
            ON t.region_key = s.region_key
            WHEN MATCHED AND t.region != s.region THEN UPDATE SET region = s.region
            WHEN NOT MATCHED THEN INSERT (region_key, region) VALUES (s.region_key, s.region)
            """)
            self._optimize_table("dim_region", ["region_key"]) 
        else:
            print("dim_region_silver not found; leaving dim_region as-is.")

        # dim_tower (SCD Type-2)
        try:
            spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.dim_tower
            (
                tower_key BIGINT GENERATED ALWAYS AS IDENTITY,
                tower_sk STRING,
                tower_id STRING,
                region_key INT,
                latitude DOUBLE,
                longitude DOUBLE,
                effective_date TIMESTAMP,
                expiry_date TIMESTAMP,
                is_current BOOLEAN,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
            USING DELTA
            PARTITIONED BY (is_current)
            LOCATION '{self.gold_path}/dim_tower'
            TBLPROPERTIES (delta.enableChangeDataFeed = true)
            """)
        except Exception as e:
            print(f"Warning creating dim_tower with IDENTITY/CDF: {e}. Retrying with simpler schema.")
            self.dim_tower_has_identity = False
            spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.dim_tower
            (
                tower_key BIGINT,
                tower_sk STRING,
                tower_id STRING,
                region_key INT,
                latitude DOUBLE,
                longitude DOUBLE,
                effective_date TIMESTAMP,
                expiry_date TIMESTAMP,
                is_current BOOLEAN,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            ) USING DELTA
            PARTITIONED BY (is_current)
            LOCATION '{self.gold_path}/dim_tower'
            """)
        if self._table_exists("tower_locations") and self._validate_columns("tower_locations", ["tower_sk", "tower_id", "region_key", "latitude", "longitude"]):
            towers_silver = spark.read.table(f"{self.catalog}.tower_locations") \
                .withColumn("effective_date", current_timestamp()) \
                .withColumn("expiry_date", lit("2999-12-31 23:59:59").cast("timestamp")) \
                .withColumn("is_current", lit(True)) \
                .withColumn("created_at", current_timestamp()) \
                .withColumn("updated_at", current_timestamp()) \
                .withColumn("tower_key", abs(hash(col("tower_sk"))).cast("bigint"))
            towers_silver.createOrReplaceTempView("stg_dim_tower")
            # Phase 1: expire changed current rows
            spark.sql(f"""
            MERGE INTO {self.catalog}.dim_tower t
            USING (SELECT tower_sk, tower_id, region_key, latitude, longitude FROM stg_dim_tower) s
            ON t.tower_sk = s.tower_sk AND t.is_current = true
            WHEN MATCHED AND (
                COALESCE(t.region_key, -1) <> COALESCE(s.region_key, -1) OR
                COALESCE(t.latitude, 0.0) <> COALESCE(s.latitude, 0.0) OR
                COALESCE(t.longitude, 0.0) <> COALESCE(s.longitude, 0.0)
            ) THEN UPDATE SET
                t.expiry_date = current_timestamp(),
                t.is_current = false,
                t.updated_at = current_timestamp()
            """)
            # Phase 2: insert new current rows where no current version exists
            if self.dim_tower_has_identity:
                spark.sql(f"""
                MERGE INTO {self.catalog}.dim_tower t
                USING (
                    SELECT tower_sk, tower_id, region_key, latitude, longitude,
                           current_timestamp() AS effective_date,
                           CAST('2999-12-31 23:59:59' AS TIMESTAMP) AS expiry_date,
                           true AS is_current,
                           current_timestamp() AS created_at,
                           current_timestamp() AS updated_at
                    FROM stg_dim_tower
                ) s
                ON t.tower_sk = s.tower_sk AND t.is_current = true
                WHEN NOT MATCHED THEN INSERT (tower_sk, tower_id, region_key, latitude, longitude, effective_date, expiry_date, is_current, created_at, updated_at)
                VALUES (s.tower_sk, s.tower_id, s.region_key, s.latitude, s.longitude, s.effective_date, s.expiry_date, s.is_current, s.created_at, s.updated_at)
                """)
            else:
                spark.sql(f"""
                MERGE INTO {self.catalog}.dim_tower t
                USING (
                    SELECT tower_key, tower_sk, tower_id, region_key, latitude, longitude,
                           current_timestamp() AS effective_date,
                           CAST('2999-12-31 23:59:59' AS TIMESTAMP) AS expiry_date,
                           true AS is_current,
                           current_timestamp() AS created_at,
                           current_timestamp() AS updated_at
                    FROM stg_dim_tower
                ) s
                ON t.tower_sk = s.tower_sk AND t.is_current = true
                WHEN NOT MATCHED THEN INSERT (tower_key, tower_sk, tower_id, region_key, latitude, longitude, effective_date, expiry_date, is_current, created_at, updated_at)
                VALUES (s.tower_key, s.tower_sk, s.tower_id, s.region_key, s.latitude, s.longitude, s.effective_date, s.expiry_date, s.is_current, s.created_at, s.updated_at)
                """)
            self._optimize_table("dim_tower", ["is_current", "region_key"]) 
        else:
            print("tower_locations silver table not found; skipping dim_tower population.")

        # dim_date
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.dim_date
        USING DELTA
        LOCATION '{self.gold_path}/dim_date'
        AS SELECT * FROM (
            SELECT CAST(NULL AS INT) AS date_key, CAST(NULL AS DATE) AS date_actual, 
            CAST(NULL AS INT) AS year, CAST(NULL AS INT) AS quarter, CAST(NULL AS INT) AS month, 
            CAST(NULL AS INT) AS day, CAST(NULL AS INT) AS hour_key, CAST(NULL AS STRING) AS day_name, 
            CAST(NULL AS STRING) AS month_name, CAST(NULL AS STRING) AS weekday_indicator, 
            CAST(NULL AS INT) AS fiscal_year, CAST(NULL AS INT) AS fiscal_month, CAST(NULL AS BOOLEAN) AS is_holiday
        ) WHERE 1=0
        """)
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2028, 12, 31)
        date_range = spark.range(0, (end_date - start_date).days * 24 + 24).select(
            date_add(lit(start_date), (col("id") / 24).cast("int")).alias("date_actual"),
            (col("id") % 24).alias("hour_key")
        )
        dim_date = date_range.select(
            date_format(col("date_actual"), "yyyyMMdd").cast("int").alias("date_key"),
            col("date_actual"),
            year(col("date_actual")).alias("year"),
            quarter(col("date_actual")).alias("quarter"),
            month(col("date_actual")).alias("month"),
            dayofmonth(col("date_actual")).alias("day"),
            col("hour_key"),
            date_format(col("date_actual"), "EEEE").alias("day_name"),
            date_format(col("date_actual"), "MMMM").alias("month_name"),
            when(dayofweek(col("date_actual")).isin([1, 7]), "Weekend").otherwise("Weekday").alias("weekday_indicator"),
            when(month(col("date_actual")) >= 4, year(col("date_actual"))).otherwise(year(col("date_actual")) - 1).alias("fiscal_year"),
            when(month(col("date_actual")) >= 4, month(col("date_actual")) - 3).otherwise(month(col("date_actual")) + 9).alias("fiscal_month"),
            when(
                (month(col("date_actual")) == 1) & (dayofmonth(col("date_actual")) == 1) |
                (month(col("date_actual")) == 3) & (dayofmonth(col("date_actual")) == 21) |
                (month(col("date_actual")) == 4) & (dayofmonth(col("date_actual")) == 27) |
                (month(col("date_actual")) == 5) & (dayofmonth(col("date_actual")) == 1) |
                (month(col("date_actual")) == 6) & (dayofmonth(col("date_actual")) == 16) |
                (month(col("date_actual")) == 8) & (dayofmonth(col("date_actual")) == 9) |
                (month(col("date_actual")) == 9) & (dayofmonth(col("date_actual")) == 24) |
                (month(col("date_actual")) == 12) & (dayofmonth(col("date_actual")).isin([16, 25, 26])),
                True
            ).otherwise(False).alias("is_holiday"),
            current_timestamp().alias("created_at")
        )
        dim_date.createOrReplaceTempView("stg_dim_date")
        spark.sql(f"""
        MERGE INTO {self.catalog}.dim_date t
        USING (SELECT DISTINCT date_key, date_actual, year, quarter, month, day, hour_key, day_name, month_name, weekday_indicator, fiscal_year, fiscal_month, is_holiday, created_at FROM stg_dim_date) s
        ON t.date_key = s.date_key AND t.hour_key = s.hour_key
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """)

        # dim_customer
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.dim_customer
        USING DELTA
        LOCATION '{self.gold_path}/dim_customer'
        AS SELECT * FROM (
            SELECT CAST(NULL AS BIGINT) AS customer_key, CAST(NULL AS STRING) AS customer_sk, 
            CAST(NULL AS STRING) AS customer_type, CAST(NULL AS STRING) AS customer_segment, 
            CAST(NULL AS STRING) AS usage_category, CAST(NULL AS INT) AS credit_rating, 
            CAST(NULL AS DATE) AS registration_date, CAST(NULL AS BOOLEAN) AS is_active,
            CAST(NULL AS TIMESTAMP) AS created_at, CAST(NULL AS TIMESTAMP) AS updated_at
        ) WHERE 1=0
        """)
        if self._table_exists("customer_usage") and self._validate_columns("customer_usage", ["customer_sk", "date_key", "hour_key", "ts_utc", "data_usage_mb", "call_duration_min", "usage_category"]):
            cu_silver = spark.read.table(f"{self.catalog}.customer_usage")
            dim_customer = cu_silver.select("customer_sk").distinct() \
                .withColumn("customer_key", row_number().over(Window.orderBy("customer_sk"))) \
                .withColumn("customer_type", when(hash(col("customer_sk")) % 100 < 20, "Prepaid").otherwise("Postpaid")) \
                .withColumn("customer_segment", when(hash(col("customer_sk")) % 100 < 30, "Individual")
                            .when(hash(col("customer_sk")) % 100 < 60, "Family").otherwise("Business")) \
                .withColumn("usage_category", when(hash(col("customer_sk")) % 100 < 25, "Light")
                            .when(hash(col("customer_sk")) % 100 < 70, "Medium").otherwise("Heavy")) \
                .withColumn("credit_rating", (abs(hash(col("customer_sk"))) % 5 + 1)) \
                .withColumn("registration_date", date_sub(current_date(), (abs(hash(col("customer_sk"))) % 1095).cast("int"))) \
                .withColumn("is_active", lit(True)) \
                .withColumn("created_at", current_timestamp()) \
                .withColumn("updated_at", current_timestamp())
            dim_customer.createOrReplaceTempView("stg_dim_customer")
            spark.sql(f"""
            MERGE INTO {self.catalog}.dim_customer t
            USING (SELECT DISTINCT customer_key, customer_sk, customer_type, customer_segment, usage_category, credit_rating, registration_date, is_active, created_at, updated_at FROM stg_dim_customer) s
            ON t.customer_sk = s.customer_sk
            WHEN MATCHED AND (t.customer_type != s.customer_type OR t.customer_segment != s.customer_segment OR t.usage_category != s.usage_category OR t.credit_rating != s.credit_rating OR t.is_active != s.is_active) THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """)
            self._optimize_table("dim_customer", ["customer_sk"]) 
        else:
            print("customer_usage silver table not found; skipping dim_customer population.")

        # dim_technician
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.dim_technician
        USING DELTA
        LOCATION '{self.gold_path}/dim_technician'
        AS SELECT * FROM (
            SELECT CAST(NULL AS BIGINT) AS technician_key, CAST(NULL AS STRING) AS technician_sk, 
            CAST(NULL AS STRING) AS role, CAST(NULL AS BOOLEAN) AS is_active,
            CAST(NULL AS TIMESTAMP) AS created_at, CAST(NULL AS TIMESTAMP) AS updated_at
        ) WHERE 1=0
        """)
        if self._table_exists("voice_transcriptions") and self._validate_columns("voice_transcriptions", ["technician_sk", "date_key", "hour_key", "ts_utc", "transcription"]):
            vt_silver = spark.read.table(f"{self.catalog}.voice_transcriptions")
            dim_technician = vt_silver.select("technician_sk").distinct() \
                .withColumn("technician_key", row_number().over(Window.orderBy("technician_sk"))) \
                .withColumn("role", when(hash(col("technician_sk")) % 100 < 50, "Field Technician").otherwise("Network Engineer")) \
                .withColumn("is_active", lit(True)) \
                .withColumn("created_at", current_timestamp()) \
                .withColumn("updated_at", current_timestamp())
            dim_technician.createOrReplaceTempView("stg_dim_technician")
            spark.sql(f"""
            MERGE INTO {self.catalog}.dim_technician t
            USING (SELECT DISTINCT technician_key, technician_sk, role, is_active, created_at, updated_at FROM stg_dim_technician) s
            ON t.technician_sk = s.technician_sk
            WHEN MATCHED AND (t.role != s.role OR t.is_active != s.is_active) THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """)
            self._optimize_table("dim_technician", ["technician_sk"]) 
        else:
            print("voice_transcriptions silver table not found; skipping dim_technician population.")

        # dim_crew
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.dim_crew
        USING DELTA
        LOCATION '{self.gold_path}/dim_crew'
        AS SELECT * FROM (
            SELECT CAST(NULL AS BIGINT) AS crew_key, CAST(NULL AS STRING) AS crew_sk, 
            CAST(NULL AS STRING) AS crew_type, CAST(NULL AS BOOLEAN) AS is_active,
            CAST(NULL AS TIMESTAMP) AS created_at, CAST(NULL AS TIMESTAMP) AS updated_at
        ) WHERE 1=0
        """)
        if self._table_exists("maintenance_crew") and self._validate_columns("maintenance_crew", ["crew_sk", "date_key", "region_key", "available"]):
            mc_silver = spark.read.table(f"{self.catalog}.maintenance_crew")
            dim_crew = mc_silver.select("crew_sk").distinct() \
                .withColumn("crew_key", row_number().over(Window.orderBy("crew_sk"))) \
                .withColumn("crew_type", when(hash(col("crew_sk")) % 100 < 50, "Maintenance").otherwise("Emergency")) \
                .withColumn("is_active", lit(True)) \
                .withColumn("created_at", current_timestamp()) \
                .withColumn("updated_at", current_timestamp())
            dim_crew.createOrReplaceTempView("stg_dim_crew")
            spark.sql(f"""
            MERGE INTO {self.catalog}.dim_crew t
            USING (SELECT DISTINCT crew_key, crew_sk, crew_type, is_active, created_at, updated_at FROM stg_dim_crew) s
            ON t.crew_sk = s.crew_sk
            WHEN MATCHED AND (t.crew_type != s.crew_type OR t.is_active != s.is_active) THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """)
            self._optimize_table("dim_crew", ["crew_sk"]) 
        else:
            print("maintenance_crew silver table not found; skipping dim_crew population.")

    def create_fact_tables(self):
        print("Creating fact tables…")

        # fact_network_performance
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.fact_network_performance
        USING DELTA
        PARTITIONED BY (date_key)
        LOCATION '{self.gold_path}/fact_network_performance'
        AS SELECT * FROM (
            SELECT CAST(NULL AS BIGINT) AS tower_key, CAST(NULL AS INT) AS region_key, CAST(NULL AS INT) AS date_key, 
            CAST(NULL AS INT) AS hour_key, CAST(NULL AS TIMESTAMP) AS ts_utc, CAST(NULL AS DOUBLE) AS signal_strength, 
            CAST(NULL AS DOUBLE) AS latency_ms, CAST(NULL AS DOUBLE) AS uptime, CAST(NULL AS STRING) AS error_code, 
            CAST(NULL AS STRING) AS performance_category, CAST(NULL AS DOUBLE) AS temperature_c, 
            CAST(NULL AS DOUBLE) AS humidity_percent, CAST(NULL AS DOUBLE) AS wind_speed_mps, 
            CAST(NULL AS DOUBLE) AS visibility_km, CAST(NULL AS DOUBLE) AS dew_point_c, 
            CAST(NULL AS INT) AS weather_severity, CAST(NULL AS INT) AS load_shedding_indicator,
            CAST(NULL AS TIMESTAMP) AS created_at, CAST(NULL AS TIMESTAMP) AS updated_at
        ) WHERE 1=0
        """)
        if (
            self._table_exists("network_logs") and
            self._validate_columns("network_logs", [
                "tower_sk", "region_key", "date_key", "hour_key", "ts_utc",
                "signal_strength", "latency_ms", "uptime", "error_code", "performance_category"
            ]) and
            self._table_exists("dim_tower") and self._table_exists("dim_date")
        ):
            nw_silver = spark.read.table(f"{self.catalog}.network_logs").alias("n")
            dim_tower_cur = spark.read.table(f"{self.catalog}.dim_tower").filter(col("is_current") == True).alias("dt")
            dim_date = spark.read.table(f"{self.catalog}.dim_date").alias("dd")
            df = nw_silver.join(dim_tower_cur, col("n.tower_sk") == col("dt.tower_sk"), "inner") \
                .join(dim_date, (col("n.date_key") == col("dd.date_key")) & (col("n.hour_key") == col("dd.hour_key")), "inner")
            if self._table_exists("weather_data") and self._validate_columns("weather_data", [
                "tower_sk", "ts_utc", "temperature_c", "humidity_percent", "wind_speed_mps", "visibility_km", "dew_point_c", "weather_severity"
            ]):
                weather_silver = spark.read.table(f"{self.catalog}.weather_data").alias("w")
                df = df.join(weather_silver, (col("n.tower_sk") == col("w.tower_sk")) & (date_trunc("hour", col("n.ts_utc")) == date_trunc("hour", col("w.ts_utc"))), "left")
            if self._table_exists("load_shedding_schedules") and self._validate_columns("load_shedding_schedules", [
                "region_key", "start_utc", "end_utc"
            ]):
                ls_silver = spark.read.table(f"{self.catalog}.load_shedding_schedules").alias("ls")
                df = df.join(ls_silver, (col("n.region_key") == col("ls.region_key")) & (col("n.ts_utc").between(col("ls.start_utc"), col("ls.end_utc"))), "left")
            fact_network = df.select(
                col("dt.tower_key").alias("tower_key"), col("n.region_key"), col("n.date_key"), col("n.hour_key"), col("n.ts_utc"),
                col("n.signal_strength"), col("n.latency_ms"), col("n.uptime"), col("n.error_code"), col("n.performance_category"),
                coalesce(col("w.temperature_c"), lit(25.0)).alias("temperature_c"),
                coalesce(col("w.humidity_percent"), lit(60.0)).alias("humidity_percent"),
                coalesce(col("w.wind_speed_mps"), lit(5.0)).alias("wind_speed_mps"),
                coalesce(col("w.visibility_km"), lit(10.0)).alias("visibility_km"),
                col("w.dew_point_c"), col("w.weather_severity"),
                when(col("ls.ls_row_sk").isNotNull(), lit(1)).otherwise(lit(0)).alias("load_shedding_indicator"),
                current_timestamp().alias("created_at"), current_timestamp().alias("updated_at")
            )
            fact_network.createOrReplaceTempView("stg_fact_network")
            spark.sql(f"""
            MERGE INTO {self.catalog}.fact_network_performance t
            USING (SELECT * FROM stg_fact_network) s
            ON t.tower_key = s.tower_key AND t.date_key = s.date_key AND t.hour_key = s.hour_key AND t.ts_utc = s.ts_utc
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """)
            self._optimize_table("fact_network_performance", ["date_key", "tower_key"]) 
        else:
            print("Skipping fact_network_performance due to missing prerequisites.")

        # fact_customer_usage
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.fact_customer_usage
        USING DELTA
        PARTITIONED BY (date_key)
        LOCATION '{self.gold_path}/fact_customer_usage'
        AS SELECT * FROM (
            SELECT CAST(NULL AS BIGINT) AS customer_key, CAST(NULL AS INT) AS date_key, CAST(NULL AS INT) AS hour_key, 
            CAST(NULL AS TIMESTAMP) AS ts_utc, CAST(NULL AS DOUBLE) AS data_usage_mb, 
            CAST(NULL AS DOUBLE) AS call_duration_minutes, CAST(NULL AS STRING) AS usage_category, 
            CAST(NULL AS DOUBLE) AS estimated_revenue_zar, CAST(NULL AS INT) AS heavy_user_flag,
            CAST(NULL AS TIMESTAMP) AS created_at, CAST(NULL AS TIMESTAMP) AS updated_at
        ) WHERE 1=0
        """)
        if (
            self._table_exists("customer_usage") and
            self._validate_columns("customer_usage", ["customer_sk", "date_key", "hour_key", "ts_utc", "data_usage_mb", "call_duration_min", "usage_category"]) and
            self._table_exists("dim_customer") and self._table_exists("dim_date")
        ):
            cu_silver = spark.read.table(f"{self.catalog}.customer_usage").alias("cu")
            dim_customer = spark.read.table(f"{self.catalog}.dim_customer").alias("dc")
            dim_date = spark.read.table(f"{self.catalog}.dim_date").alias("dd")
            fact_usage = cu_silver \
                .join(dim_customer, col("cu.customer_sk") == col("dc.customer_sk"), "inner") \
                .join(dim_date, (col("cu.date_key") == col("dd.date_key")) & (col("cu.hour_key") == col("dd.hour_key")), "inner") \
                .select(
                    col("dc.customer_key").alias("customer_key"), col("cu.date_key"), col("cu.hour_key"), col("cu.ts_utc"),
                    col("cu.data_usage_mb"), col("cu.call_duration_min").alias("call_duration_minutes"),
                    col("cu.usage_category"), round(col("cu.data_usage_mb") * lit(0.05), 2).alias("estimated_revenue_zar"),
                    when(col("cu.data_usage_mb") > 1000, lit(1)).otherwise(lit(0)).alias("heavy_user_flag"),
                    current_timestamp().alias("created_at"), current_timestamp().alias("updated_at")
                )
            fact_usage.createOrReplaceTempView("stg_fact_usage")
            spark.sql(f"""
            MERGE INTO {self.catalog}.fact_customer_usage t
            USING (SELECT * FROM stg_fact_usage) s
            ON t.customer_key = s.customer_key AND t.date_key = s.date_key AND t.hour_key = s.hour_key AND t.ts_utc = s.ts_utc
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """)
            self._optimize_table("fact_customer_usage", ["date_key", "customer_key"]) 
        else:
            print("Skipping fact_customer_usage due to missing prerequisites.")

        # fact_load_shedding_slots_30min
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.fact_load_shedding_slots_30min
        USING DELTA
        PARTITIONED BY (date_key)
        LOCATION '{self.gold_path}/fact_load_shedding_slots_30min'
        AS SELECT * FROM (
            SELECT CAST(NULL AS INT) AS region_key, CAST(NULL AS INT) AS date_key, 
            CAST(NULL AS TIMESTAMP) AS slot_start_utc, CAST(NULL AS TIMESTAMP) AS slot_end_utc,
            CAST(NULL AS TIMESTAMP) AS created_at
        ) WHERE 1=0
        """)
        if self._table_exists("load_shedding_schedules") and self._validate_columns("load_shedding_schedules", ["region_key", "start_utc", "end_utc"]):
            ls_silver = spark.read.table(f"{self.catalog}.load_shedding_schedules")
            fact_ls_slots = ls_silver \
                .withColumn("slots", expr("sequence(start_utc, end_utc, interval 30 minutes)")) \
                .withColumn("slot_start_utc", explode(col("slots"))) \
                .withColumn("slot_end_utc", col("slot_start_utc") + expr("interval 30 minutes")) \
                .withColumn("date_key", date_format(col("slot_start_utc"), "yyyyMMdd").cast("int")) \
                .select("region_key", "date_key", "slot_start_utc", "slot_end_utc", current_timestamp().alias("created_at"))
            fact_ls_slots.createOrReplaceTempView("stg_fact_ls_slots")
            spark.sql(f"""
            MERGE INTO {self.catalog}.fact_load_shedding_slots_30min t
            USING (SELECT * FROM stg_fact_ls_slots) s
            ON t.region_key = s.region_key AND t.date_key = s.date_key AND t.slot_start_utc = s.slot_start_utc
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """)
            self._optimize_table("fact_load_shedding_slots_30min", ["date_key", "region_key"]) 
        else:
            print("Skipping fact_load_shedding_slots_30min due to missing load_shedding_schedules.")

        # fact_customer_feedback
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.fact_customer_feedback
        USING DELTA
        PARTITIONED BY (date_key)
        LOCATION '{self.gold_path}/fact_customer_feedback'
        AS SELECT * FROM (
            SELECT CAST(NULL AS INT) AS date_key, CAST(NULL AS INT) AS hour_key, 
            CAST(NULL AS TIMESTAMP) AS ts_utc, CAST(NULL AS STRING) AS text, 
            CAST(NULL AS DOUBLE) AS sentiment_score, CAST(NULL AS STRING) AS sentiment_label,
            CAST(NULL AS TIMESTAMP) AS created_at
        ) WHERE 1=0
        """)
        if (
            self._table_exists("customer_feedback") and self._validate_columns("customer_feedback", ["date_key", "hour_key", "ts_utc", "text", "sentiment_score", "sentiment_label"]) and
            self._table_exists("dim_date")
        ):
            cf_silver = spark.read.table(f"{self.catalog}.customer_feedback").alias("cf")
            dim_date = spark.read.table(f"{self.catalog}.dim_date").alias("dd")
            fact_feedback = cf_silver \
                .join(dim_date, (col("cf.date_key") == col("dd.date_key")) & (col("cf.hour_key") == col("dd.hour_key")), "inner") \
                .select(
                    col("dd.date_key"), col("cf.hour_key"), col("cf.ts_utc"),
                    col("cf.text"), col("cf.sentiment_score"), col("cf.sentiment_label"),
                    current_timestamp().alias("created_at")
                )
            fact_feedback.createOrReplaceTempView("stg_fact_feedback")
            spark.sql(f"""
            MERGE INTO {self.catalog}.fact_customer_feedback t
            USING (SELECT * FROM stg_fact_feedback) s
            ON t.date_key = s.date_key AND t.hour_key = s.hour_key AND t.ts_utc = s.ts_utc AND t.text = s.text
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """)
            self._optimize_table("fact_customer_feedback", ["date_key"]) 
        else:
            print("Skipping fact_customer_feedback due to missing prerequisites.")

        # fact_tower_connectivity
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.fact_tower_connectivity
        USING DELTA
        PARTITIONED BY (date_key)
        LOCATION '{self.gold_path}/fact_tower_connectivity'
        AS SELECT * FROM (
            SELECT CAST(NULL AS BIGINT) AS src_tower_key, CAST(NULL AS BIGINT) AS dst_tower_key, 
            CAST(NULL AS INT) AS date_key, CAST(NULL AS INT) AS hour_key, CAST(NULL AS TIMESTAMP) AS ts_utc, 
            CAST(NULL AS DOUBLE) AS signal_quality,
            CAST(NULL AS TIMESTAMP) AS created_at
        ) WHERE 1=0
        """)
        if (
            self._table_exists("tower_connectivity") and self._validate_columns("tower_connectivity", ["src_tower_sk", "dst_tower_sk", "date_key", "hour_key", "ts_utc", "signal_quality"]) and
            self._table_exists("dim_tower") and self._table_exists("dim_date")
        ):
            tc_silver = spark.read.table(f"{self.catalog}.tower_connectivity").alias("tc")
            dim_tower_cur = spark.read.table(f"{self.catalog}.dim_tower").filter(col("is_current") == True)
            src = dim_tower_cur.alias("src")
            dst = dim_tower_cur.alias("dst")
            dim_date = spark.read.table(f"{self.catalog}.dim_date").alias("dd")
            fact_connectivity = tc_silver \
                .join(src, col("tc.src_tower_sk") == col("src.tower_sk"), "inner") \
                .join(dst, col("tc.dst_tower_sk") == col("dst.tower_sk"), "inner") \
                .join(dim_date, (col("tc.date_key") == col("dd.date_key")) & (col("tc.hour_key") == col("dd.hour_key")), "inner") \
                .select(
                    col("src.tower_key").alias("src_tower_key"), col("dst.tower_key").alias("dst_tower_key"),
                    col("tc.date_key"), col("tc.hour_key"), col("tc.ts_utc"),
                    col("tc.signal_quality"), current_timestamp().alias("created_at")
                )
            fact_connectivity.createOrReplaceTempView("stg_fact_connectivity")
            spark.sql(f"""
            MERGE INTO {self.catalog}.fact_tower_connectivity t
            USING (SELECT * FROM stg_fact_connectivity) s
            ON t.src_tower_key = s.src_tower_key AND t.dst_tower_key = s.dst_tower_key AND t.date_key = s.date_key AND t.ts_utc = s.ts_utc
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """)
            self._optimize_table("fact_tower_connectivity", ["date_key"]) 
        else:
            print("Skipping fact_tower_connectivity due to missing prerequisites.")

        # fact_tower_capacity
        # fact_tower_capacity (optional)
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.fact_tower_capacity
        USING DELTA
        PARTITIONED BY (date_key)
        LOCATION '{self.gold_path}/fact_tower_capacity'
        AS SELECT * FROM (
            SELECT CAST(NULL AS BIGINT) AS tower_key, CAST(NULL AS INT) AS date_key, 
            CAST(NULL AS INT) AS hour_key, CAST(NULL AS TIMESTAMP) AS ts_utc, 
            CAST(NULL AS DOUBLE) AS capacity_mbps, CAST(NULL AS DOUBLE) AS utilization_percent,
            CAST(NULL AS TIMESTAMP) AS created_at
        ) WHERE 1=0
        """)
        if (
            self._table_exists("tower_capacity") and self._validate_columns("tower_capacity", ["tower_sk", "date_key", "hour_key", "ts_utc", "capacity_mbps", "utilization_percent"]) and
            self._table_exists("dim_tower") and self._table_exists("dim_date")
        ):
            tcap_silver = spark.read.table(f"{self.catalog}.tower_capacity").alias("tcap")
            dim_tower_cur = spark.read.table(f"{self.catalog}.dim_tower").filter(col("is_current") == True).alias("dt")
            dim_date = spark.read.table(f"{self.catalog}.dim_date").alias("dd")
            fact_capacity = tcap_silver \
                .join(dim_tower_cur, col("tcap.tower_sk") == col("dt.tower_sk"), "inner") \
                .join(dim_date, (col("tcap.date_key") == col("dd.date_key")) & (col("tcap.hour_key") == col("dd.hour_key")), "inner") \
                .select(
                    col("dt.tower_key").alias("tower_key"), col("tcap.date_key"), col("tcap.hour_key"),
                    col("tcap.ts_utc"), col("tcap.capacity_mbps"), col("tcap.utilization_percent"),
                    current_timestamp().alias("created_at")
                )
            fact_capacity.createOrReplaceTempView("stg_fact_capacity")
            spark.sql(f"""
            MERGE INTO {self.catalog}.fact_tower_capacity t
            USING (SELECT * FROM stg_fact_capacity) s
            ON t.tower_key = s.tower_key AND t.date_key = s.date_key AND t.hour_key = s.hour_key AND t.ts_utc = s.ts_utc
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """)
            self._optimize_table("fact_tower_capacity", ["date_key"]) 
        else:
            print("Skipping population of fact_tower_capacity due to missing prerequisites.")

        # fact_maintenance_activity
        # fact_maintenance_activity (optional)
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.fact_maintenance_activity
        USING DELTA
        PARTITIONED BY (date_key)
        LOCATION '{self.gold_path}/fact_maintenance_activity'
        AS SELECT * FROM (
            SELECT CAST(NULL AS BIGINT) AS technician_key, CAST(NULL AS INT) AS date_key, 
            CAST(NULL AS INT) AS hour_key, CAST(NULL AS TIMESTAMP) AS ts_utc, 
            CAST(NULL AS STRING) AS transcription, CAST(NULL AS STRING) AS activity_type,
            CAST(NULL AS TIMESTAMP) AS created_at
        ) WHERE 1=0
        """)
        if self._table_exists("voice_transcriptions") and self._table_exists("dim_technician") and self._table_exists("dim_date"):
            vt_silver = spark.read.table(f"{self.catalog}.voice_transcriptions").alias("vt")
            dim_technician = spark.read.table(f"{self.catalog}.dim_technician").alias("dtech")
            dim_date = spark.read.table(f"{self.catalog}.dim_date").alias("dd")
            fact_maintenance = vt_silver \
                .join(dim_technician, col("vt.technician_sk") == col("dtech.technician_sk"), "inner") \
                .join(dim_date, (col("vt.date_key") == col("dd.date_key")) & (col("vt.hour_key") == col("dd.hour_key")), "inner") \
                .withColumn("activity_type", 
                    when(col("transcription").contains("INSPECTION"), "Inspection")
                    .when(col("transcription").contains("REPLACE") | col("transcription").contains("REPAIR"), "Repair")
                    .when(col("transcription").contains("ESCALATE"), "Escalation")
                    .otherwise("Other")) \
                .select(
                    col("dtech.technician_key").alias("technician_key"), col("vt.date_key"), col("vt.hour_key"),
                    col("vt.ts_utc"), col("vt.transcription"), col("activity_type"),
                    current_timestamp().alias("created_at")
                )
            fact_maintenance.createOrReplaceTempView("stg_fact_maintenance")
            spark.sql(f"""
            MERGE INTO {self.catalog}.fact_maintenance_activity t
            USING (SELECT * FROM stg_fact_maintenance) s
            ON t.technician_key = s.technician_key AND t.date_key = s.date_key AND t.hour_key = s.hour_key AND t.ts_utc = s.ts_utc
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """)
            self._optimize_table("fact_maintenance_activity", ["date_key"]) 
        else:
            print("Skipping population of fact_maintenance_activity due to missing prerequisites.")

    def create_aggregate_tables(self):
        print("Creating aggregate tables…")
        # Daily network summary per tower
        spark.sql(f"""
        CREATE OR REPLACE TABLE {self.catalog}.agg_daily_network_summary
        USING DELTA
        LOCATION '{self.gold_path}/agg_daily_network_summary'
        PARTITIONED BY (date_key)
        AS
        SELECT 
            fnp.date_key, dr.region, dt.tower_id, dt.latitude, dt.longitude,
            COUNT(*) as total_measurements, AVG(fnp.signal_strength) as avg_signal_strength,
            MIN(fnp.signal_strength) as min_signal_strength, MAX(fnp.signal_strength) as max_signal_strength,
            AVG(fnp.latency_ms) as avg_latency_ms, AVG(fnp.uptime) as avg_uptime,
            SUM(CASE WHEN fnp.performance_category = 'GOOD' THEN 1 ELSE 0 END) as good_signal_count,
            SUM(CASE WHEN fnp.latency_ms <= 50 THEN 1 ELSE 0 END) as low_latency_count,
            SUM(CASE WHEN fnp.uptime >= 99 THEN 1 ELSE 0 END) as high_uptime_count,
            SUM(CASE WHEN fnp.error_code != 'NONE' THEN 1 ELSE 0 END) as error_count,
            ROUND(SUM(CASE WHEN fnp.performance_category = 'GOOD' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as signal_quality_pct,
            ROUND(SUM(CASE WHEN fnp.latency_ms <= 50 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as low_latency_pct,
            ROUND(SUM(CASE WHEN fnp.uptime >= 99 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as high_uptime_pct,
            ROUND(SUM(CASE WHEN fnp.error_code != 'NONE' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as error_rate_pct,
            AVG(fnp.temperature_c) as avg_temperature, AVG(fnp.humidity_percent) as avg_humidity,
            AVG(fnp.wind_speed_mps) as avg_wind_speed, AVG(fnp.dew_point_c) as avg_dew_point_c,
            AVG(tcap.capacity_mbps) as avg_capacity_mbps, AVG(tcap.utilization_percent) as avg_utilization_percent,
            current_timestamp() as created_at
        FROM {self.catalog}.fact_network_performance fnp
        JOIN {self.catalog}.dim_tower dt ON fnp.tower_key = dt.tower_key AND dt.is_current = true
        LEFT JOIN {self.catalog}.dim_region dr ON fnp.region_key = dr.region_key
        LEFT JOIN {self.catalog}.fact_tower_capacity tcap ON fnp.tower_key = tcap.tower_key AND fnp.date_key = tcap.date_key AND fnp.hour_key = tcap.hour_key
        GROUP BY fnp.date_key, dr.region, dt.tower_id, dt.latitude, dt.longitude
        """)
        spark.sql(f"""
        CREATE OR REPLACE TABLE {self.catalog}.agg_hourly_usage_summary
        USING DELTA
        LOCATION '{self.gold_path}/agg_hourly_usage_summary'
        PARTITIONED BY (date_key)
        AS
        SELECT
            fcu.date_key, fcu.hour_key, dc.customer_segment, dc.customer_type, dc.usage_category,
            COUNT(DISTINCT fcu.customer_key) as active_customers, SUM(fcu.data_usage_mb) as total_data_usage_mb,
            SUM(fcu.call_duration_minutes) as total_call_minutes, SUM(fcu.estimated_revenue_zar) as total_estimated_revenue,
            AVG(fcu.data_usage_mb) as avg_data_usage_per_customer, AVG(fcu.call_duration_minutes) as avg_call_duration_per_customer,
            AVG(fcu.estimated_revenue_zar) as avg_revenue_per_customer, SUM(fcu.heavy_user_flag) as heavy_users_count,
            AVG(fcf.sentiment_score) as avg_sentiment_score,
            SUM(CASE WHEN fcf.sentiment_label = 'POSITIVE' THEN 1 ELSE 0 END) as positive_feedback_count,
            SUM(CASE WHEN fcf.sentiment_label = 'NEGATIVE' THEN 1 ELSE 0 END) as negative_feedback_count,
            current_timestamp() as created_at
        FROM {self.catalog}.fact_customer_usage fcu
        JOIN {self.catalog}.dim_customer dc ON fcu.customer_key = dc.customer_key AND dc.is_active = true
        LEFT JOIN {self.catalog}.fact_customer_feedback fcf ON fcu.date_key = fcf.date_key AND fcu.hour_key = fcf.hour_key
        GROUP BY fcu.date_key, fcu.hour_key, dc.customer_segment, dc.customer_type, dc.usage_category
        """)
        spark.sql(f"""
        CREATE OR REPLACE TABLE {self.catalog}.agg_regional_performance
        USING DELTA
        LOCATION '{self.gold_path}/agg_regional_performance'
        PARTITIONED BY (date_key)
        AS
        SELECT
            dns.date_key, dns.region, dd.date_actual, dd.day_name, dd.is_holiday,
            COUNT(DISTINCT dns.tower_id) as total_towers, AVG(dns.avg_signal_strength) as region_avg_signal_strength,
            AVG(dns.avg_latency_ms) as region_avg_latency, AVG(dns.avg_uptime) as region_avg_uptime,
            AVG(dns.signal_quality_pct) as region_signal_quality_score, AVG(dns.low_latency_pct) as region_latency_quality_score,
            AVG(dns.high_uptime_pct) as region_uptime_quality_score, AVG(dns.error_rate_pct) as region_error_rate,
            AVG(dns.avg_capacity_mbps) as region_avg_capacity_mbps, AVG(dns.avg_utilization_percent) as region_avg_utilization_percent,
            ROUND((AVG(dns.signal_quality_pct) * 0.4 + AVG(dns.low_latency_pct) * 0.3 + AVG(dns.high_uptime_pct) * 0.3), 2) as network_health_score,
            AVG(dns.avg_temperature) as region_avg_temperature, AVG(dns.avg_humidity) as region_avg_humidity,
            current_timestamp() as created_at
        FROM {self.catalog}.agg_daily_network_summary dns
        JOIN {self.catalog}.dim_date dd ON dns.date_key = dd.date_key
        GROUP BY dns.date_key, dns.region, dd.date_actual, dd.day_name, dd.is_holiday
        """)
        # Optional crew availability aggregate (only if maintenance_crew exists)
        if self._table_exists("maintenance_crew"):
            spark.sql(f"""
            CREATE OR REPLACE TABLE {self.catalog}.agg_crew_availability
            USING DELTA
            LOCATION '{self.gold_path}/agg_crew_availability'
            PARTITIONED BY (date_key)
            AS
            SELECT
                mc.date_key, mc.region_key, dr.region, COUNT(DISTINCT mc.crew_sk) as total_crews,
                SUM(CASE WHEN mc.available THEN 1 ELSE 0 END) as available_crews,
                ROUND(SUM(CASE WHEN mc.available THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT mc.crew_sk), 2) as availability_pct,
                current_timestamp() as created_at
            FROM {self.catalog}.maintenance_crew mc
            JOIN {self.catalog}.dim_region dr ON mc.region_key = dr.region_key
            GROUP BY mc.date_key, mc.region_key, dr.region
            """)
        else:
            print("agg_crew_availability skipped: maintenance_crew table not found.")

    def create_views(self):
        print("Creating views…")
        # Network health insights view
        if self._table_exists("agg_regional_performance"):
            if self._table_exists("agg_crew_availability"):
                spark.sql(f"""
                CREATE OR REPLACE VIEW {self.catalog}.vw_network_health_insights AS
                SELECT
                    rp.date_key, rp.region, rp.date_actual, rp.day_name, rp.is_holiday,
                    rp.region_avg_signal_strength, rp.region_avg_latency, rp.region_avg_uptime,
                    rp.network_health_score, rp.region_avg_capacity_mbps, rp.region_avg_utilization_percent,
                    ca.availability_pct as crew_availability_pct,
                    COUNT(DISTINCT fcf.text) as feedback_count,
                    AVG(fcf.sentiment_score) as avg_feedback_sentiment,
                    SUM(CASE WHEN fcf.sentiment_label = 'NEGATIVE' THEN 1 ELSE 0 END) as negative_feedback_count,
                    COUNT(DISTINCT tc.src_tower_key) as connected_towers
                FROM {self.catalog}.agg_regional_performance rp
                LEFT JOIN {self.catalog}.agg_crew_availability ca ON rp.date_key = ca.date_key AND rp.region = ca.region
                LEFT JOIN {self.catalog}.fact_customer_feedback fcf ON rp.date_key = fcf.date_key
                LEFT JOIN {self.catalog}.fact_tower_connectivity tc ON rp.date_key = tc.date_key
                GROUP BY
                    rp.date_key, rp.region, rp.date_actual, rp.day_name, rp.is_holiday,
                    rp.region_avg_signal_strength, rp.region_avg_latency, rp.region_avg_uptime,
                    rp.network_health_score, rp.region_avg_capacity_mbps, rp.region_avg_utilization_percent,
                    ca.availability_pct
                """)
            else:
                spark.sql(f"""
                CREATE OR REPLACE VIEW {self.catalog}.vw_network_health_insights AS
                SELECT
                    rp.date_key, rp.region, rp.date_actual, rp.day_name, rp.is_holiday,
                    rp.region_avg_signal_strength, rp.region_avg_latency, rp.region_avg_uptime,
                    rp.network_health_score, rp.region_avg_capacity_mbps, rp.region_avg_utilization_percent,
                    NULL as crew_availability_pct,
                    COUNT(DISTINCT fcf.text) as feedback_count,
                    AVG(fcf.sentiment_score) as avg_feedback_sentiment,
                    SUM(CASE WHEN fcf.sentiment_label = 'NEGATIVE' THEN 1 ELSE 0 END) as negative_feedback_count,
                    COUNT(DISTINCT tc.src_tower_key) as connected_towers
                FROM {self.catalog}.agg_regional_performance rp
                LEFT JOIN {self.catalog}.fact_customer_feedback fcf ON rp.date_key = fcf.date_key
                LEFT JOIN {self.catalog}.fact_tower_connectivity tc ON rp.date_key = tc.date_key
                GROUP BY
                    rp.date_key, rp.region, rp.date_actual, rp.day_name, rp.is_holiday,
                    rp.region_avg_signal_strength, rp.region_avg_latency, rp.region_avg_uptime,
                    rp.network_health_score, rp.region_avg_capacity_mbps, rp.region_avg_utilization_percent
                """)
        else:
            print("vw_network_health_insights skipped: agg_regional_performance not found.")

        # Maintenance operations view (optional)
        if self._table_exists("fact_maintenance_activity") and self._table_exists("dim_technician"):
            spark.sql(f"""
            CREATE OR REPLACE VIEW {self.catalog}.vw_maintenance_operations AS
            SELECT
                ma.date_key, ma.hour_key, dt.technician_key, dt.role, ma.transcription, ma.activity_type,
                tc.signal_quality, tcap.utilization_percent
            FROM {self.catalog}.fact_maintenance_activity ma
            JOIN {self.catalog}.dim_technician dt ON ma.technician_key = dt.technician_key
            LEFT JOIN {self.catalog}.fact_tower_connectivity tc ON ma.date_key = tc.date_key AND ma.hour_key = tc.hour_key
            LEFT JOIN {self.catalog}.fact_tower_capacity tcap ON ma.date_key = tcap.date_key AND ma.hour_key = tcap.hour_key
            WHERE dt.is_active = true
            """)
        else:
            print("vw_maintenance_operations skipped: dependencies missing.")

        # Data quality monitoring view
        spark.sql(f"""
        CREATE OR REPLACE VIEW {self.catalog}.vw_data_quality_monitoring AS
        SELECT
            'fact_network_performance' as table_name, date_key,
            COUNT(*) as row_count,
            SUM(CASE WHEN signal_strength IS NULL OR signal_strength < 0 OR signal_strength > 100 THEN 1 ELSE 0 END) as invalid_signal_strength,
            SUM(CASE WHEN latency_ms IS NULL OR latency_ms < 0 OR latency_ms > 1000 THEN 1 ELSE 0 END) as invalid_latency_ms,
            SUM(CASE WHEN uptime IS NULL OR uptime < 0 OR uptime > 100 THEN 1 ELSE 0 END) as invalid_uptime
        FROM {self.catalog}.fact_network_performance
        GROUP BY date_key
        UNION ALL
        SELECT
            'fact_customer_usage' as table_name, date_key,
            COUNT(*) as row_count,
            SUM(CASE WHEN data_usage_mb IS NULL OR data_usage_mb < 0 THEN 1 ELSE 0 END) as invalid_data_usage,
            SUM(CASE WHEN call_duration_minutes IS NULL OR call_duration_minutes < 0 THEN 1 ELSE 0 END) as invalid_call_duration
        FROM {self.catalog}.fact_customer_usage
        GROUP BY date_key
        UNION ALL
        SELECT
            'fact_customer_feedback' as table_name, date_key,
            COUNT(*) as row_count,
            SUM(CASE WHEN sentiment_score IS NULL OR sentiment_score < -1 OR sentiment_score > 1 THEN 1 ELSE 0 END) as invalid_sentiment_score,
            SUM(CASE WHEN sentiment_label IS NULL OR sentiment_label NOT IN ('POSITIVE', 'NEGATIVE', 'NEUTRAL') THEN 1 ELSE 0 END) as invalid_sentiment_label
        FROM {self.catalog}.fact_customer_feedback
        GROUP BY date_key
        UNION ALL
        SELECT
            'fact_tower_connectivity' as table_name, date_key,
            COUNT(*) as row_count,
            SUM(CASE WHEN signal_quality IS NULL OR signal_quality < 0 OR signal_quality > 100 THEN 1 ELSE 0 END) as invalid_signal_quality,
            NULL as placeholder
        FROM {self.catalog}.fact_tower_connectivity
        GROUP BY date_key
        UNION ALL
        SELECT
            'fact_tower_capacity' as table_name, date_key,
            COUNT(*) as row_count,
            SUM(CASE WHEN capacity_mbps IS NULL OR capacity_mbps < 0 THEN 1 ELSE 0 END) as invalid_capacity_mbps,
            SUM(CASE WHEN utilization_percent IS NULL OR utilization_percent < 0 OR utilization_percent > 100 THEN 1 ELSE 0 END) as invalid_utilization_percent
        FROM {self.catalog}.fact_tower_capacity
        GROUP BY date_key
        """)

    def generate_gold_summary(self):
        print("Generating gold layer summary…")
        tables_to_check = [
            "dim_region", "dim_tower", "dim_date", "dim_customer", "dim_technician", "dim_crew",
            "fact_network_performance", "fact_customer_usage", "fact_load_shedding_slots_30min",
            "fact_customer_feedback", "fact_tower_connectivity", "fact_tower_capacity",
            "fact_maintenance_activity", "agg_daily_network_summary", "agg_hourly_usage_summary",
            "agg_regional_performance", "agg_crew_availability"
        ]
        table_stats = {}
        for table in tables_to_check:
            try:
                count = spark.table(f"{self.catalog}.{table}").count()
                table_stats[table] = count
                print(f"- {table}: {count:,} rows")
            except Exception as e:
                print(f"! Could not get count for {table}: {e}")
        views_to_check = ["vw_network_health_insights", "vw_maintenance_operations", "vw_data_quality_monitoring"]
        for view in views_to_check:
            try:
                count = spark.sql(f"SELECT * FROM {self.catalog}.{view}").count()
                print(f"- {view}: {count:,} rows")
            except Exception as e:
                print(f"! Could not get count for {view}: {e}")
        print("Gold Layer Summary: 17 tables and up to 3 views targeted.")

    def run_silver_to_gold_pipeline(self):
        print("Starting Silver to Gold Pipeline…")
    # Confirm Delta capabilities up-front
    self.check_delta_capabilities()
    self.create_dimension_tables()
    self.create_fact_tables()
    self.create_aggregate_tables()
    self.create_views()
    self.generate_gold_summary()

# Run pipeline directly when `spark` is available (Databricks)
if spark is not None:
    pipeline = TelkomSilverToGold()
    pipeline.run_silver_to_gold_pipeline()
else:
    print("Databricks global 'spark' not detected; skipping pipeline run.")
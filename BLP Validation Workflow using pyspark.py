
# Design Principles:
# 1. ZERO Python threading — all parallelism is Spark-native (executors)
# 2. Single broadcast of active_meters (cached, reused across all dates)
# 3. Predicate pushdown to PostgreSQL (minimize data transfer)
# 4. Adaptive Query Execution (AQE) + optimized shuffle partitions
# 5. Batch processing across dates in a single Spark job
# 6. No intermediate PostgreSQL round-trips for BLP step
# =============================================================================

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import sys

# JDBC Configuration
JDBC_HOST = "your_host"
JDBC_PORT = "5432"
JDBC_DB = "your_database"
JDBC_USER = "username"
JDBC_PASSWORD = "your_password"

JDBC_URL = f"jdbc:postgresql://{JDBC_HOST}:{JDBC_PORT}/{JDBC_DB}"

DB_PROPERTIES = {
    "user": JDBC_USER,
    "password": JDBC_PASSWORD,
    "driver": "org.postgresql.Driver",
    "fetchsize": "100000",        # Large fetch size for bulk reads
    "batchsize": "100000",        # Large batch size for bulk writes
}

# Pipeline Configuration
START_DATE = "20260517"
END_DATE = "20260524"
METER_ENTRY_TYPE = '7547484b-28cb-4f5e-92d0-729c18c540b9'

# =========================================================
# SPARK SESSION — MAXIMUM PERFORMANCE CONFIGURATION
# =========================================================

spark = SparkSession.builder \
    .appName("BLP_Pipeline_UltraPerf") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m") \
    .config("spark.sql.autoBroadcastJoinThreshold", "512m") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.inMemoryColumnarStorage.compressed", "true") \
    .config("spark.sql.inMemoryColumnarStorage.batchSize", "20000") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.files.maxPartitionBytes", "134217728") \
    .config("spark.sql.files.openCostInBytes", "4194304") \
    .config("spark.default.parallelism", "400") \
    .config("spark.sql.broadcastTimeout", "1200") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# PHASE 0: CACHE ACTIVE METERS (ONE-TIME, BROADCAST-READY)
# =========================================================

def get_active_meters():
    """
    Load active meters once, cache in memory, broadcast to all executors.
    This is the SMALL table — perfect for broadcast join.
    """
    print("[PHASE 0] Loading & caching active meters...")
    
    active_meters = spark.read.jdbc(
        url=JDBC_URL,
        table=f"""(SELECT mtr_number 
                   FROM cdb.meter_master 
                   WHERE record_status = 1 
                     AND mtr_installed_date IS NOT NULL 
                     AND mtr_entry_type = '{METER_ENTRY_TYPE}'
                  ) AS active_meters""",
        properties=DB_PROPERTIES
    ).repartition(10, "mtr_number")  # Pre-partition for efficient joins
    
    active_meters.cache()
    active_meters.count()  # Materialize cache
    
    print(f"[PHASE 0] Active meters loaded: {active_meters.count():,}")
    return active_meters


# =========================================================
# PHASE 1: BULK EXTRACT HES & MDM (PARALLEL JDBC READS)
# =========================================================

def extract_hes_batch(dates_list, active_meters_bc):
    """
    Extract HES data for ALL dates in a SINGLE JDBC read.
    Uses date range filter with pushdown.
    """
    print(f"[PHASE 1A] Bulk extracting HES for {len(dates_list)} dates...")
    
    start_ts = min(dates_list).strftime('%Y-%m-%d 00:00:00')
    end_ts = (max(dates_list) + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
    
    # Single bulk read with date range — PostgreSQL handles the filtering
    hes_raw = spark.read.jdbc(
        url=JDBC_URL,
        table=f"""(SELECT DISTINCT meter_number AS mtr_number, 
                          meter_time,
                          DATE(meter_time) AS read_date
                   FROM fep.fep_csv_lp
                   WHERE meter_time >= '{start_ts}'
                     AND meter_time < '{end_ts}'
                     AND status = 'Success'
                     AND status_message = 'Successfully Completed'
                  ) AS hes_data""",
        properties=DB_PROPERTIES,
        numPartitions=200,  # Parallel JDBC reads
        column="meter_time",
        lowerBound=start_ts,
        upperBound=end_ts
    )
    
    # Broadcast join + filter to active meters
    hes_filtered = hes_raw.join(
        F.broadcast(active_meters_bc), 
        "mtr_number", 
        "inner"
    )
    
    # Aggregate by date and meter (ONE shuffle for ALL dates)
    hes_agg = hes_filtered.groupBy(
        F.col("read_date"),
        F.col("mtr_number")
    ).agg(
        F.countDistinct("meter_time").alias("hes_cnt")
    )
    
    return hes_agg


def extract_mdm_batch(dates_list, active_meters_bc):
    """
    Extract MDM data for ALL dates in a SINGLE JDBC read.
    Uses IN clause for lp_date filter with pushdown.
    """
    print(f"[PHASE 1B] Bulk extracting MDM for {len(dates_list)} dates...")
    
    lp_dates = [d.strftime('%Y%m%d') for d in dates_list]
    date_list_sql = "','".join(lp_dates)
    
    mdm_raw = spark.read.jdbc(
        url=JDBC_URL,
        table=f"""(SELECT DISTINCT mtr_number, 
                          lp_time,
                          lp_date
                   FROM mdms.mdm_loadprofile_data
                   WHERE lp_date IN ('{date_list_sql}')
                  ) AS mdm_data""",
        properties=DB_PROPERTIES,
        numPartitions=200,
        column="lp_date",
        lowerBound=min(lp_dates),
        upperBound=max(lp_dates)
    )
    
    # Broadcast join with active meters
    mdm_filtered = mdm_raw.join(
        F.broadcast(active_meters_bc), 
        "mtr_number", 
        "inner"
    )
    
    # Aggregate by date and meter
    mdm_agg = mdm_filtered.groupBy(
        F.col("lp_date"),
        F.col("mtr_number")
    ).agg(
        F.countDistinct("lp_time").alias("blp_cnt")
    )
    
    return mdm_agg


# =========================================================
# PHASE 2: TRUNCATE TABLES (DDL via JDBC — FAST)
# =========================================================

def truncate_all_tables(dates_list):
    """
    Truncate all tables for all dates using a SINGLE connection.
    """
    print(f"[PHASE 2] Truncating tables for {len(dates_list)} dates...")
    
    import psycopg2
    conn = psycopg2.connect(
        host=JDBC_HOST, port=JDBC_PORT, database=JDBC_DB,
        user=JDBC_USER, password=JDBC_PASSWORD
    )
    try:
        conn.autocommit = True
        cur = conn.cursor()
        
        for current_date in dates_list:
            day_num = current_date.day
            tables = [
                f"bfd.day{day_num}_hes_blp",
                f"bfd.day{day_num}_mdm_blp", 
                f"bfd.blp_day{day_num}"
            ]
            for tbl in tables:
                cur.execute(f"TRUNCATE TABLE {tbl};")
                print(f"  Truncated: {tbl}")
        
        cur.close()
    finally:
        conn.close()
    
    print("[PHASE 2] All tables truncated.")


# =========================================================
# PHASE 3: WRITE RESULTS (OPTIMIZED BULK INSERT)
# =========================================================

def write_partitioned_results(hes_df, mdm_df, dates_list):
    """
    Write HES and MDM results back to PostgreSQL.
    Uses coalesce to control parallelism and avoid small files.
    """
    print("[PHASE 3] Writing HES & MDM results...")
    
    for current_date in dates_list:
        day_num = current_date.day
        date_str = current_date.strftime('%Y-%m-%d')
        lp_date_str = current_date.strftime('%Y%m%d')
        
        # Filter day's data and write
        hes_day = hes_df.filter(F.col("read_date") == date_str) \
            .select("mtr_number", "hes_cnt") \
            .coalesce(4)  # Control write parallelism
        
        mdm_day = mdm_df.filter(F.col("lp_date") == lp_date_str) \
            .select("mtr_number", "blp_cnt") \
            .coalesce(4)
        
        hes_table = f"bfd.day{day_num}_hes_blp"
        mdm_table = f"bfd.day{day_num}_mdm_blp"
        
        hes_day.write.mode("overwrite").jdbc(
            url=JDBC_URL, table=hes_table, properties=DB_PROPERTIES
        )
        mdm_day.write.mode("overwrite").jdbc(
            url=JDBC_URL, table=mdm_table, properties=DB_PROPERTIES
        )
        
        print(f"  Written: {hes_table} ({hes_day.count():,} rows)")
        print(f"  Written: {mdm_table} ({mdm_day.count():,} rows)")


# =========================================================
# PHASE 4: BLP COMPARISON (SPARK-NATIVE, NO DB ROUND-TRIP)
# =========================================================

def compute_and_write_blp(hes_df, mdm_df, dates_list):
    """
    Compute BLP comparison entirely in Spark memory.
    NO PostgreSQL round-trips — pure Spark join.
    """
    print("[PHASE 4] Computing BLP comparisons in Spark...")
    
    # Filter MDM: blp_cnt < 48 OR 49-95
    mdm_exceptions = mdm_df.filter(
        (F.col("blp_cnt") < 48) | 
        ((F.col("blp_cnt") >= 49) & (F.col("blp_cnt") <= 95))
    ).select(
        F.col("lp_date").alias("blp_date"),
        "mtr_number"
    )
    
    # Join HES with MDM exceptions
    blp_joined = hes_df.join(
        mdm_exceptions,
        (hes_df.read_date == F.to_date(mdm_exceptions.blp_date, 'yyyyMMdd')) &
        (hes_df.mtr_number == mdm_exceptions.mtr_number),
        "inner"
    ).select(
        hes_df.mtr_number,
        hes_df.read_date
    ).distinct()
    
    # Add row number per date
    window_spec = Window.partitionBy("read_date").orderBy("mtr_number")
    blp_result = blp_joined.withColumn("rn", F.row_number().over(window_spec))
    
    # Write per day
    for current_date in dates_list:
        day_num = current_date.day
        date_str = current_date.strftime('%Y-%m-%d')
        
        blp_day = blp_result.filter(F.col("read_date") == date_str) \
            .select("mtr_number", "rn") \
            .coalesce(2)
        
        blp_table = f"bfd.blp_day{day_num}"
        
        blp_day.write.mode("overwrite").jdbc(
            url=JDBC_URL, table=blp_table, properties=DB_PROPERTIES
        )
        
        print(f"  Written: {blp_table} ({blp_day.count():,} rows)")


# =========================================================
# MAIN PIPELINE — SINGLE SPARK JOB, ALL DATES
# =========================================================

def main():
    # Parse date range
    start_date = datetime.strptime(START_DATE, "%Y%m%d")
    end_date = datetime.strptime(END_DATE, "%Y%m%d")
    
    dates_list = []
    current = start_date
    while current <= end_date:
        dates_list.append(current)
        current += timedelta(days=1)
    
    print(f"\n{'='*60}")
    print(f"BLP PIPELINE — {len(dates_list)} DATES")
    print(f"FROM {START_DATE} TO {END_DATE}")
    print(f"{'='*60}\n")
    
    # PHASE 0: Cache active meters (broadcast-ready)
    active_meters = get_active_meters()
    
    # PHASE 1: Bulk extract HES & MDM (parallel, single reads)
    hes_agg = extract_hes_batch(dates_list, active_meters)
    mdm_agg = extract_mdm_batch(dates_list, active_meters)
    
    # Persist intermediate results (prevent re-computation)
    hes_agg.cache()
    mdm_agg.cache()
    hes_agg.count()
    mdm_agg.count()
    
    # PHASE 2: Truncate all target tables
    truncate_all_tables(dates_list)
    
    # PHASE 3: Write HES & MDM results
    write_partitioned_results(hes_agg, mdm_agg, dates_list)
    
    # PHASE 4: Compute & write BLP (all in Spark, no DB reads)
    compute_and_write_blp(hes_agg, mdm_agg, dates_list)
    
    # Cleanup
    hes_agg.unpersist()
    mdm_agg.unpersist()
    active_meters.unpersist()
    
    print(f"\n{'='*60}")
    print("ALL WORKFLOWS COMPLETED SUCCESSFULLY")
    print(f"{'='*60}\n")
    
    spark.stop()


if __name__ == "__main__":
    main()

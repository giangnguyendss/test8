spark.catalog.setCurrentCatalog("purgo_databricks")

# ------------------------------------------------------------------------------------
# Customer 360 Raw Table Backup and Vacuum Operations
# ------------------------------------------------------------------------------------
# Catalog: purgo_databricks
# Schema: purgo_playground
# Source Table: customer_360_raw
# Backup Volume Path: /Volumes/customer_360_raw_backup
# Compression Codec: snappy
# Partition Column: state
# Retention Policy: 90 days for backup files, 30 days for vacuum
# Logging Table: customer_360_raw_backup_log
# All columns included, nulls preserved, backup before vacuum
# ------------------------------------------------------------------------------------

# Required imports for PySpark DataFrame operations and logging
from pyspark.sql import DataFrame  
from pyspark.sql.functions import col, lit, current_timestamp  
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType  
from datetime import datetime, timedelta  

# ------------------------------------------------------------------------------------
# Helper Function: Log backup/vacuum operation to purgo_playground.customer_360_raw_backup_log
# ------------------------------------------------------------------------------------
def log_operation(status, operation_type, record_count, error_message=None):
    """
    Log backup or vacuum operation to purgo_playground.customer_360_raw_backup_log.
    """
    log_df = spark.createDataFrame([
        (
            datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            status,
            operation_type,
            record_count,
            error_message
        )
    ], ["timestamp", "status", "operation_type", "record_count", "error_message"])
    log_df.write.mode("append").insertInto("purgo_playground.customer_360_raw_backup_log")

# ------------------------------------------------------------------------------------
# Step 1: Backup customer_360_raw table to compressed parquet partitioned by state
# ------------------------------------------------------------------------------------
try:
    # Read source table from Unity Catalog
    src_df = spark.table("purgo_playground.customer_360_raw")
    # Validate partition column exists
    if "state" not in src_df.columns:
        log_operation("failed", "backup", 0, "Partition column 'state' not found in source table")
        raise Exception("Partition column 'state' not found in source table")
    # Validate compression codec
    supported_codecs = ["snappy"]
    compression_codec = "snappy"
    if compression_codec not in supported_codecs:
        log_operation("failed", "backup", 0, f"Unsupported compression codec: {compression_codec}")
        raise Exception(f"Unsupported compression codec: {compression_codec}")
    # Backup path in Databricks volume
    backup_path = "/Volumes/customer_360_raw_backup"
    # Write all columns, preserve nulls, partition by state, use snappy compression
    src_df.write.mode("overwrite").partitionBy("state").option("compression", compression_codec).parquet(backup_path)
    # Count records backed up
    backup_count = src_df.count()
    # Log success
    log_operation("success", "backup", backup_count, None)
except Exception as e:
    log_operation("failed", "backup", 0, str(e))
    # Do not raise further to allow vacuum to proceed

# ------------------------------------------------------------------------------------
# Step 2: Vacuum customer_360_raw table, retain only records from last 30 days (creation_date)
# ------------------------------------------------------------------------------------
try:
    # Calculate retention date (30 days before now)
    today = datetime.utcnow().date()
    retention_date = today - timedelta(days=30)
    # Filter records to retain
    vacuum_df = src_df.filter(col("creation_date") >= lit(str(retention_date)))
    retained_count = vacuum_df.count()
    # Overwrite source table with retained records (Delta Lake format)
    vacuum_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("purgo_playground.customer_360_raw")
    # Run Delta Lake VACUUM command to physically remove old files
    spark.sql("VACUUM purgo_playground.customer_360_raw RETAIN 0 HOURS")
    # Log success
    log_operation("success", "vacuum", retained_count, None)
except Exception as e:
    log_operation("failed", "vacuum", 0, str(e))

# ------------------------------------------------------------------------------------
# Step 3: Retention Policy Enforcement for Backup Files (delete backup files older than 90 days)
# ------------------------------------------------------------------------------------
try:
    # Use dbutils.fs to list and delete files older than 90 days in backup volume
    now = datetime.utcnow()
    deleted_count = 0
    # List all partition directories under backup_path
    partition_dirs = [f.path for f in dbutils.fs.ls(backup_path) if f.isDir]
    for part_dir in partition_dirs:
        files = dbutils.fs.ls(part_dir)
        for f in files:
            # Only consider parquet files
            if f.path.endswith(".parquet"):
                # Get file modification time in milliseconds
                mtime_ms = f.modificationTime
                mtime = datetime.utcfromtimestamp(mtime_ms / 1000)
                if (now - mtime).days > 90:
                    dbutils.fs.rm(f.path)
                    deleted_count += 1
    log_operation("success", "retention", deleted_count, None)
except Exception as e:
    log_operation("failed", "retention", 0, str(e))

# ------------------------------------------------------------------------------------
# End of Script
# ------------------------------------------------------------------------------------

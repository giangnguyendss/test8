spark.catalog.setCurrentCatalog("purgo_databricks")

# ------------------------------------------------------------------------------------
# Customer 360 Raw Table Backup and Vacuum Operations
# ------------------------------------------------------------------------------------
# Catalog: purgo_databricks
# Schema: purgo_playground
# Source Table: customer_360_raw
# Backup Volume: /Volumes/customer_360_raw_backup
# Compression Codec: snappy
# Partition Column: state
# Date Column for Vacuum: creation_date
# Backup Log Table: purgo_playground.customer_360_raw_backup_log
# Retention Policy: 90 days for backup files
# Backup Frequency: daily at 02:00 UTC
# All columns included in backup
# All operations must be logged for audit and traceability
# ------------------------------------------------------------------------------------

# -- Required imports for PySpark operations
from pyspark.sql import DataFrame  
from pyspark.sql.functions import col, current_date, expr, lit  
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType  
from datetime import datetime, timedelta  

# ------------------------------------------------------------------------------------
# SECTION: Utility Function for Logging
# ------------------------------------------------------------------------------------

def log_operation(status, operation_type, record_count, error_message=None):
    """
    Insert a log entry into purgo_playground.customer_360_raw_backup_log.
    """
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    log_df = spark.createDataFrame(
        [(timestamp, status, operation_type, record_count, error_message)],
        ["timestamp", "status", "operation_type", "record_count", "error_message"]
    )
    try:
        log_df.write.mode("append").saveAsTable("purgo_playground.customer_360_raw_backup_log")
    except Exception as e:
        # Logging failure is printed but not raised to avoid cascading errors
        print(f"# Logging failed: {e}")

# ------------------------------------------------------------------------------------
# SECTION: Backup Operation - Partitioned Parquet Write
# ------------------------------------------------------------------------------------

def backup_customer_360_raw():
    """
    Backup purgo_playground.customer_360_raw to compressed parquet partitioned by state.
    """
    try:
        # -- Read source table
        df = spark.table("purgo_playground.customer_360_raw")
        # -- Validate schema matches expected columns and types
        expected_schema = StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("company", StringType(), True),
            StructField("job_title", StringType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("country", StringType(), True),
            StructField("industry", StringType(), True),
            StructField("account_manager", StringType(), True),
            StructField("creation_date", DateType(), True),
            StructField("last_interaction_date", DateType(), True),
            StructField("purchase_history", StringType(), True),
            StructField("notes", StringType(), True),
            StructField("zip", StringType(), True)
        ])
        actual_fields = [(f.name, f.dataType.typeName(), f.nullable) for f in df.schema.fields]
        expected_fields = [(f.name, f.dataType.typeName(), f.nullable) for f in expected_schema.fields]
        if actual_fields != expected_fields:
            raise Exception(f"Schema mismatch: {actual_fields} != {expected_fields}")
        # -- Write to volume as partitioned parquet
        df.write \
            .mode("overwrite") \
            .partitionBy("state") \
            .option("compression", "snappy") \
            .parquet("/Volumes/customer_360_raw_backup/daily_backup")
        # -- Validate record count
        backup_df = spark.read.parquet("/Volumes/customer_360_raw_backup/daily_backup")
        if backup_df.count() != df.count():
            raise Exception("Backup record count mismatch")
        # -- Log success
        log_operation("success", "backup", df.count(), None)
    except Exception as e:
        # -- Log failure
        log_operation("failed", "backup", 0, str(e))
        print(f"# Backup failed: {e}")

# ------------------------------------------------------------------------------------
# SECTION: Vacuum Operation - Retain Last 30 Days
# ------------------------------------------------------------------------------------

def vacuum_customer_360_raw():
    """
    Vacuum purgo_playground.customer_360_raw, retaining only records from last 30 days.
    """
    try:
        # -- Read source table
        df = spark.table("purgo_playground.customer_360_raw")
        # -- Check if creation_date column exists
        if "creation_date" not in df.columns:
            raise Exception("Column creation_date not found.")
        # -- Filter for last 30 days
        filtered_df = df.filter(col("creation_date") >= expr("date_sub(current_date(), 30)"))
        # -- Overwrite table with filtered data
        filtered_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("purgo_playground.customer_360_raw")
        # -- Log success
        log_operation("success", "vacuum", filtered_df.count(), None)
    except Exception as e:
        # -- Log failure
        log_operation("failed", "vacuum", 0, str(e))
        print(f"# Vacuum failed: {e}")

# ------------------------------------------------------------------------------------
# SECTION: Retention Policy Enforcement for Backup Files
# ------------------------------------------------------------------------------------

def enforce_backup_retention():
    """
    Delete backup parquet files older than 90 days in /Volumes/customer_360_raw_backup/daily_backup.
    """
    deleted_count = 0
    try:
        # -- Use dbutils.fs to list and delete files in the backup volume
        cutoff_ts = (datetime.utcnow() - timedelta(days=90)).timestamp()
        backup_base = "/Volumes/customer_360_raw_backup/daily_backup"
        # List all partition folders (state=XX)
        for state_folder in dbutils.fs.ls(backup_base):
            state_path = state_folder.path
            # List all files in partition folder
            for file_info in dbutils.fs.ls(state_path):
                # Only consider parquet files
                if file_info.name.endswith(".parquet"):
                    # Get file modification time
                    mtime = file_info.modificationTime / 1000.0  # ms to seconds
                    if mtime < cutoff_ts:
                        try:
                            dbutils.fs.rm(file_info.path)
                            deleted_count += 1
                        except Exception as e:
                            print(f"# Error deleting file {file_info.path}: {e}")
        # -- Log success
        log_operation("success", "retention", deleted_count, None)
    except Exception as e:
        # -- Log failure
        log_operation("failed", "retention", 0, str(e))
        print(f"# Retention enforcement failed: {e}")

# ------------------------------------------------------------------------------------
# SECTION: Data Quality Validation
# ------------------------------------------------------------------------------------

def validate_data_quality():
    """
    Validate data quality for backup and vacuum operations.
    """
    try:
        df = spark.table("purgo_playground.customer_360_raw")
        # -- Invalid creation_date format
        invalid_creation_date = df.filter((~col("creation_date").cast("date").isNotNull()) & (col("creation_date").isNotNull()))
        if invalid_creation_date.count() > 0:
            log_operation("failed", "vacuum", 0, "creation_date value format invalid.")
        # -- Invalid email format (simple regex)
        invalid_email = df.filter((~col("email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")) & (col("email").isNotNull()))
        if invalid_email.count() > 0:
            log_operation("failed", "backup", 0, "email value format invalid.")
        # -- id as string or null
        invalid_id = df.filter(~col("id").cast("bigint").isNotNull())
        if invalid_id.count() > 0:
            log_operation("failed", "backup", 0, "id value format invalid.")
    except Exception as e:
        log_operation("failed", "data_quality", 0, str(e))
        print(f"# Data quality validation failed: {e}")

# ------------------------------------------------------------------------------------
# SECTION: Main Execution
# ------------------------------------------------------------------------------------

if __name__ == "__main__":
    # -- Step 1: Backup operation
    backup_customer_360_raw()
    # -- Step 2: Vacuum operation
    vacuum_customer_360_raw()
    # -- Step 3: Retention enforcement
    enforce_backup_retention()
    # -- Step 4: Data quality validation
    validate_data_quality()
    # -- End of script
    # All operations completed

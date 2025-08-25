# ------------------------------------------------------------------------------------
# Databricks PySpark Script: Backup customer_360_raw Table to Partitioned Parquet & Vacuum
# ------------------------------------------------------------------------------------
# This script performs the following:
#   1. Reads all data from purgo_databricks.purgo_playground.customer_360_raw
#   2. Writes the data as compressed parquet files (snappy) partitioned by 'state'
#      to /Volumes/customer_360_raw_backup/
#   3. Performs a Delta Lake VACUUM operation on the original table, retaining only
#      records from the last 30 days (creation_date >= current_date - 30)
#   4. Implements error handling, schema validation, and Databricks best practices
# ------------------------------------------------------------------------------------
# Setup: Required imports for PySpark DataFrame, types, and functions
# from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType  
from pyspark.sql.functions import col, current_date, date_sub  
from pyspark.sql.utils import AnalysisException  

# ------------------------------------------------------------------------------------
# Step 1: Define explicit schema matching purgo_playground.customer_360_raw
# ------------------------------------------------------------------------------------
customer_360_raw_schema = StructType([
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

# ------------------------------------------------------------------------------------
# Step 2: Read source table from Unity Catalog
# ------------------------------------------------------------------------------------
try:
    # Set current catalog for Unity Catalog operations
    spark.catalog.setCurrentCatalog("purgo_databricks")
    # Read the source table
    df_customer_360_raw = spark.table("purgo_playground.customer_360_raw")
except AnalysisException as e:
    # Error: Source table does not exist or cannot be read
    raise RuntimeError(f"Error reading source table: {e}")

# ------------------------------------------------------------------------------------
# Step 3: Validate schema and column count before backup
# ------------------------------------------------------------------------------------
src_cols = df_customer_360_raw.columns
expected_cols = [f.name for f in customer_360_raw_schema.fields]
if src_cols != expected_cols:
    raise ValueError(f"Column mismatch: Source columns {src_cols} do not match expected schema {expected_cols}")

# ------------------------------------------------------------------------------------
# Step 4: Validate and convert data types to match schema
# ------------------------------------------------------------------------------------
for field in customer_360_raw_schema.fields:
    col_name = field.name
    col_type = field.dataType
    # If type mismatch, cast to correct type
    if df_customer_360_raw.schema[col_name].dataType != col_type:
        df_customer_360_raw = df_customer_360_raw.withColumn(col_name, col(col_name).cast(col_type))

# ------------------------------------------------------------------------------------
# Step 5: Backup to partitioned, compressed parquet in volume
# ------------------------------------------------------------------------------------
backup_path = "/Volumes/customer_360_raw_backup/"
try:
    # Check if partition column exists
    if "state" not in df_customer_360_raw.columns:
        raise ValueError("Partition column 'state' not found in source table")
    # Write as partitioned parquet with snappy compression
    df_customer_360_raw.write.mode("overwrite").partitionBy("state").parquet(
        backup_path, compression="snappy"
    )
    # Comment: Backup written to partitioned parquet with snappy compression
except Exception as e:
    # Error: Backup location missing, not writable, or other write error
    raise RuntimeError(f"Error writing backup parquet: {e}")

# ------------------------------------------------------------------------------------
# Step 6: Delta Lake VACUUM - Retain only records from last 30 days
# ------------------------------------------------------------------------------------
try:
    # Calculate retention date (current_date - 30)
    retention_date = date_sub(current_date(), 30)
    # Check if creation_date column exists
    if "creation_date" not in df_customer_360_raw.columns:
        raise ValueError("Date column 'creation_date' not found in source table")
    # Filter records to retain only those within retention period
    df_vacuumed = df_customer_360_raw.filter(col("creation_date") >= retention_date)
    # Overwrite the original table with vacuumed data
    df_vacuumed.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable("purgo_playground.customer_360_raw")
    # Run Delta Lake VACUUM command to physically remove old files
    spark.sql("VACUUM purgo_playground.customer_360_raw RETAIN 720 HOURS")
    # Comment: Vacuum operation completed, old records removed
except Exception as e:
    # Error: Vacuum failed due to missing date column, permissions, or other error
    raise RuntimeError(f"Error during vacuum operation: {e}")

# ------------------------------------------------------------------------------------
# End of Databricks PySpark Script for Backup and Vacuum Operations
# ------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------
# Databricks PySpark Script: Automate migration of eligible files from Purgo S3 landing folder to archive folder
# -----------------------------------------------------------------------------------
# This script migrates files from S3 landing to archive folder using s3_file_process_log table.
# Only files with file_status = 'SUCCESS' are migrated.
# AWS credentials are read from Databricks secret scope 'aws_keys'.
# Migration events are logged with file_id, file_name, source, target, status, and timestamp.
# Error handling, S3 path validation, and audit logging are implemented.
# -----------------------------------------------------------------------------------

# from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql.types import StructType, StructField, StringType, TimestampType  
from pyspark.sql.functions import col, lit, current_timestamp, regexp_extract, when  
import re  
import datetime  

# -----------------------------------------------------------------------------------
# Section: Setup and Configuration
# -----------------------------------------------------------------------------------
# Set current catalog and schema for Unity Catalog
spark.catalog.setCurrentCatalog("purgo_databricks")
spark.catalog.setCurrentDatabase("purgo_playground")

# -----------------------------------------------------------------------------------
# Section: AWS Credentials Retrieval
# -----------------------------------------------------------------------------------
try:
    access_key = dbutils.secrets.get(scope="aws_keys", key="access_key")  # Databricks built-in
    secret_key = dbutils.secrets.get(scope="aws_keys", key="secret_key")  # Databricks built-in
except Exception as e:
    # Log error and terminate if credentials are missing
    print("ERROR: AWS credentials not found in Databricks secret scope: aws_keys")
    raise

# -----------------------------------------------------------------------------------
# Section: S3 Path Validation Function
# -----------------------------------------------------------------------------------
# Regex for S3 path validation
s3_regex = r"^s3://[a-zA-Z0-9\-]+(/[a-zA-Z0-9_\-]+)+$"

def is_valid_s3_path(path):
    if path is None:
        return False
    return re.match(s3_regex, path) is not None

from pyspark.sql.functions import udf  
from pyspark.sql.types import BooleanType  
is_valid_s3_path_udf = udf(is_valid_s3_path, BooleanType())

# -----------------------------------------------------------------------------------
# Section: Read Eligible Files from Log Table
# -----------------------------------------------------------------------------------
# CTE to select eligible files for migration
cte_query = """
WITH eligible_files AS (
  SELECT
    file_id,
    file_name,
    file_status,
    s3_landing_path,
    s3_archive_path,
    processed_timestamp
  FROM purgo_databricks.purgo_playground.s3_file_process_log
  WHERE file_status = 'SUCCESS'
)
SELECT * FROM eligible_files
"""

df_eligible = spark.sql(cte_query)

# -----------------------------------------------------------------------------------
# Section: Data Quality Checks and S3 Path Validation
# -----------------------------------------------------------------------------------
df_eligible = df_eligible \
    .withColumn("landing_path_valid", is_valid_s3_path_udf(col("s3_landing_path"))) \
    .withColumn("archive_path_valid", is_valid_s3_path_udf(col("s3_archive_path"))) \
    .withColumn("file_id_valid", col("file_id").isNotNull()) \
    .withColumn("file_name_valid", col("file_name").isNotNull())

# Filter out invalid records and log errors
df_valid = df_eligible.filter(
    col("landing_path_valid") & col("archive_path_valid") & col("file_id_valid") & col("file_name_valid")
)

df_invalid = df_eligible.filter(
    ~col("landing_path_valid") | ~col("archive_path_valid") | ~col("file_id_valid") | ~col("file_name_valid")
)

# Log invalid records
invalid_records = df_invalid.select(
    "file_id", "file_name", "s3_landing_path", "s3_archive_path",
    when(~col("landing_path_valid"), lit("Invalid S3 landing path format"))
    .when(~col("archive_path_valid"), lit("Invalid S3 archive path format"))
    .when(~col("file_id_valid"), lit("Missing file_id"))
    .when(~col("file_name_valid"), lit("Missing file_name"))
    .otherwise(lit("Unknown error")).alias("error_message"),
    current_timestamp().alias("logged_timestamp")
)

if invalid_records.count() > 0:
    invalid_records.show(truncate=False)  # For audit, can be written to a log table

# -----------------------------------------------------------------------------------
# Section: S3 File Migration Logic
# -----------------------------------------------------------------------------------
# Helper function to move file from S3 landing to archive using dbutils.fs.cp and dbutils.fs.rm
def move_s3_file(src_path, tgt_path, file_name, file_id):
    try:
        # Compose full source and target file paths
        src_file = src_path.rstrip("/") + "/" + file_name
        tgt_file = tgt_path.rstrip("/") + "/" + file_name

        # Check if source file exists
        src_exists = False
        try:
            src_list = dbutils.fs.ls(src_path.rstrip("/") + "/")
            src_exists = any(f.name == file_name for f in src_list)
        except Exception as e:
            return {
                "file_id": file_id,
                "file_name": file_name,
                "source": src_file,
                "target": tgt_file,
                "status": "ERROR",
                "message": f"File not found in source S3 path for file_id: {file_id}, file_name: {file_name}",
                "processed_timestamp": datetime.datetime.utcnow()
            }
        if not src_exists:
            return {
                "file_id": file_id,
                "file_name": file_name,
                "source": src_file,
                "target": tgt_file,
                "status": "ERROR",
                "message": f"File not found in source S3 path for file_id: {file_id}, file_name: {file_name}",
                "processed_timestamp": datetime.datetime.utcnow()
            }

        # Check if target file exists
        tgt_exists = False
        try:
            tgt_list = dbutils.fs.ls(tgt_path.rstrip("/") + "/")
            tgt_exists = any(f.name == file_name for f in tgt_list)
        except Exception as e:
            # If target folder does not exist, create it
            try:
                dbutils.fs.mkdirs(tgt_path.rstrip("/") + "/")
            except Exception as e_mkdir:
                return {
                    "file_id": file_id,
                    "file_name": file_name,
                    "source": src_file,
                    "target": tgt_file,
                    "status": "ERROR",
                    "message": f"Failed to create target S3 folder for file_id: {file_id}, error: {str(e_mkdir)}",
                    "processed_timestamp": datetime.datetime.utcnow()
                }

        # Copy file from source to target (overwrite if exists)
        try:
            dbutils.fs.cp(src_file, tgt_file, True)
            status = "ARCHIVED"
            message = "Migration successful"
            if tgt_exists:
                status = "WARNING"
                message = f"File overwritten in archive S3 path for file_id: {file_id}, file_name: {file_name}"
        except Exception as e:
            return {
                "file_id": file_id,
                "file_name": file_name,
                "source": src_file,
                "target": tgt_file,
                "status": "ERROR",
                "message": f"Insufficient S3 permissions or copy failed for file_id: {file_id}, error: {str(e)}",
                "processed_timestamp": datetime.datetime.utcnow()
            }

        # Delete source file after successful copy
        try:
            dbutils.fs.rm(src_file)
        except Exception as e:
            # Log but do not fail migration if delete fails
            message += f" | Source file deletion failed: {str(e)}"

        return {
            "file_id": file_id,
            "file_name": file_name,
            "source": src_file,
            "target": tgt_file,
            "status": status,
            "message": message,
            "processed_timestamp": datetime.datetime.utcnow()
        }
    except Exception as e:
        return {
            "file_id": file_id,
            "file_name": file_name,
            "source": src_path,
            "target": tgt_path,
            "status": "ERROR",
            "message": f"Unexpected error for file_id: {file_id}, error: {str(e)}",
            "processed_timestamp": datetime.datetime.utcnow()
        }

# -----------------------------------------------------------------------------------
# Section: Apply Migration Logic to Valid Files
# -----------------------------------------------------------------------------------
valid_rows = df_valid.select(
    "file_id", "file_name", "s3_landing_path", "s3_archive_path"
).collect()

migration_results = []
for row in valid_rows:
    result = move_s3_file(row.s3_landing_path, row.s3_archive_path, row.file_name, row.file_id)
    migration_results.append(result)

# -----------------------------------------------------------------------------------
# Section: Audit Logging of Migration Results
# -----------------------------------------------------------------------------------
audit_schema = StructType([
    StructField("file_id", StringType(), True),
    StructField("file_name", StringType(), True),
    StructField("source", StringType(), True),
    StructField("target", StringType(), True),
    StructField("status", StringType(), True),
    StructField("message", StringType(), True),
    StructField("processed_timestamp", TimestampType(), True)
])

df_audit = spark.createDataFrame(migration_results, schema=audit_schema)

# Write audit log to Delta table (append mode)
df_audit.write.mode("append").format("delta").saveAsTable("purgo_databricks.purgo_playground.s3_file_migration_audit")

# -----------------------------------------------------------------------------------
# Section: Update Log Table for Archived Files
# -----------------------------------------------------------------------------------
from delta.tables import DeltaTable  

delta_table = DeltaTable.forName(spark, "purgo_databricks.purgo_playground.s3_file_process_log")

archived_ids = df_audit.filter(col("status") == "ARCHIVED").select("file_id").distinct()
if archived_ids.count() > 0:
    merge_source = archived_ids.withColumn("file_status", lit("ARCHIVED"))
    delta_table.alias("t").merge(
        merge_source.alias("s"),
        "t.file_id = s.file_id"
    ).whenMatchedUpdate(set={"file_status": "ARCHIVED"}).execute()

# -----------------------------------------------------------------------------------
# Section: Logging Summary
# -----------------------------------------------------------------------------------
# Log summary of migration
total_migrated = df_audit.filter(col("status") == "ARCHIVED").count()
total_overwritten = df_audit.filter(col("status") == "WARNING").count()
total_errors = df_audit.filter(col("status") == "ERROR").count()
print(f"Migration Summary: {total_migrated} files archived, {total_overwritten} files overwritten, {total_errors} errors.")

# spark.stop()  # Do not stop Spark in Databricks

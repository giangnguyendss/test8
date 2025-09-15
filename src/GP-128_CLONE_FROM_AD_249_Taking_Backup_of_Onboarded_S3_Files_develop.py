%pip install boto3
%pip install botocore

# ============================================================
# Databricks PySpark Script: S3 File Migration from Landing to Archive
# ============================================================
# Automates migration of eligible files from Purgo S3 landing folder to archive folder.
# - References Unity Catalog table: purgo_databricks.purgo_playground.s3_file_process_log
# - Moves only files with file_status = 'SUCCESS'
# - Dynamically reads s3_landing_path and s3_archive_path for each file
# - Uses Databricks secrets "access_key" and "secret_key" from scope "aws_keys" for S3 access
# - Handles error scenarios: missing files, permission issues, duplicate files in archive, invalid S3 path, NULLs, invalid file_status, identical source/target paths
# - Logs all errors and actions
# - Follows Databricks and PySpark best practices
# ============================================================

# from pyspark.sql import SparkSession  # SparkSession is already available in Databricks

# -- Required imports
from pyspark.sql.functions import col, lit, when, expr, length  
from pyspark.sql.types import StringType, TimestampType  
import re  
import sys  

# -- boto3 for S3 operations
import boto3  
from botocore.exceptions import ClientError  

# -- Databricks utilities for secrets
# dbutils is available in Databricks

# ============================================================
# Setup: AWS Credentials from Databricks Secret Scope
# ============================================================

def get_aws_credentials():
    # -- Retrieve AWS credentials from Databricks secret scope
    try:
        access_key = dbutils.secrets.get(scope="aws_keys", key="access_key")
        secret_key = dbutils.secrets.get(scope="aws_keys", key="secret_key")
        if not access_key or not secret_key:
            raise Exception("Invalid or missing AWS credentials in Databricks secret scope aws_keys")
        return access_key, secret_key
    except Exception as e:
        log_error(f"Invalid or missing AWS credentials in Databricks secret scope aws_keys: {str(e)}")
        return None, None

def log_error(msg):
    # -- Log error to stdout (can be replaced with Databricks logging)
    print(f"[ERROR] {msg}", file=sys.stderr)

def log_info(msg):
    # -- Log info to stdout
    print(f"[INFO] {msg}")

# ============================================================
# S3 Utility Functions
# ============================================================

def parse_s3_path(s3_path):
    # -- Parse S3 path into bucket and key prefix
    match = re.match(r"^s3://([^/]+)/(.+)$", s3_path or "")
    if match:
        return match.group(1), match.group(2)
    else:
        return None, None

def s3_file_exists(s3_client, s3_path, file_name):
    # -- Check if file exists in S3 path
    bucket, prefix = parse_s3_path(s3_path)
    if not bucket or not prefix or not file_name:
        return False
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=f"{prefix}/{file_name}")
        for obj in response.get("Contents", []):
            if obj["Key"].endswith(file_name):
                return True
        return False
    except Exception:
        return False

def move_s3_file(s3_client, source_path, target_path, file_name):
    # -- Move file from source to target S3 path
    src_bucket, src_prefix = parse_s3_path(source_path)
    tgt_bucket, tgt_prefix = parse_s3_path(target_path)
    if not src_bucket or not src_prefix or not tgt_bucket or not tgt_prefix or not file_name:
        return False, "Invalid S3 path format"
    src_key = f"{src_prefix}/{file_name}"
    tgt_key = f"{tgt_prefix}/{file_name}"
    try:
        s3_client.copy_object(Bucket=tgt_bucket, CopySource={"Bucket": src_bucket, "Key": src_key}, Key=tgt_key)
        s3_client.delete_object(Bucket=src_bucket, Key=src_key)
        return True, None
    except ClientError as e:
        return False, str(e)
    except Exception as e:
        return False, str(e)

# ============================================================
# CTE: Read and Validate s3_file_process_log Table
# ============================================================

# -- Set current catalog for Unity Catalog
spark.catalog.setCurrentCatalog("purgo_databricks")

# -- Define allowed file_status values
ALLOWED_FILE_STATUS = ["SUCCESS", "FAILED", "IN_PROGRESS"]

# -- Read s3_file_process_log table from Unity Catalog
s3_file_process_log_df = spark.table("purgo_playground.s3_file_process_log")

# -- Validate schema: enforce column order and types
expected_columns = [
    ("file_id", StringType()),
    ("file_name", StringType()),
    ("file_status", StringType()),
    ("s3_landing_path", StringType()),
    ("s3_archive_path", StringType()),
    ("processed_timestamp", TimestampType())
]
actual_schema = s3_file_process_log_df.schema
if len(actual_schema) != len(expected_columns):
    log_error("Column count mismatch in s3_file_process_log table")
    sys.exit(1)
for idx, (col_name, col_type) in enumerate(expected_columns):
    if actual_schema[idx].name != col_name or type(actual_schema[idx].dataType) != type(col_type):
        log_error(f"Schema mismatch at column {idx}: expected {col_name} {col_type}, got {actual_schema[idx].name} {actual_schema[idx].dataType}")
        sys.exit(1)

# -- CTE: Filter eligible files and validate data
eligible_files_cte = (
    s3_file_process_log_df
    .select(
        col("file_id").cast("string").alias("file_id"),
        col("file_name").cast("string").alias("file_name"),
        col("file_status").cast("string").alias("file_status"),
        col("s3_landing_path").cast("string").alias("s3_landing_path"),
        col("s3_archive_path").cast("string").alias("s3_archive_path"),
        col("processed_timestamp").cast("timestamp").alias("processed_timestamp")
    )
    .where(col("file_status") == "SUCCESS")
    .where(col("file_id").isNotNull() & col("file_name").isNotNull() & col("s3_landing_path").isNotNull() & col("s3_archive_path").isNotNull())
    .where(col("s3_landing_path") != col("s3_archive_path"))
    .where(col("s3_landing_path").rlike("^s3://[^/]+/.+"))
    .where(col("s3_archive_path").rlike("^s3://[^/]+/.+"))
)

# -- CTE: Invalid file_status values
invalid_status_cte = (
    s3_file_process_log_df
    .select("file_id", "file_name", "file_status")
    .where(~col("file_status").isin(ALLOWED_FILE_STATUS))
)
for row in invalid_status_cte.collect():
    log_error(f"Invalid file_status '{row.file_status}' for file {row.file_name}")

# -- CTE: Invalid S3 path format
invalid_s3_path_cte = (
    s3_file_process_log_df
    .select("file_id", "file_name", "s3_landing_path", "s3_archive_path")
    .where(~col("s3_landing_path").rlike("^s3://[^/]+/.+") | ~col("s3_archive_path").rlike("^s3://[^/]+/.+"))
)
for row in invalid_s3_path_cte.collect():
    log_error(f"Invalid S3 path format for file {row.file_name}")

# -- CTE: NULL handling
null_handling_cte = (
    s3_file_process_log_df
    .select("file_id", "file_name", "s3_landing_path", "s3_archive_path")
    .where(col("file_name").isNull() | col("s3_landing_path").isNull() | col("s3_archive_path").isNull())
)
for row in null_handling_cte.collect():
    log_error(f"NULL value detected for file_id {row.file_id}")

# -- CTE: Identical source and target S3 paths
identical_path_cte = (
    s3_file_process_log_df
    .select("file_id", "file_name", "s3_landing_path", "s3_archive_path")
    .where(col("s3_landing_path") == col("s3_archive_path"))
)
for row in identical_path_cte.collect():
    log_error(f"Source and target S3 paths are identical for file {row.file_name}")

# ============================================================
# S3 File Migration Logic
# ============================================================

# -- Get AWS credentials
access_key, secret_key = get_aws_credentials()
if not access_key or not secret_key:
    sys.exit(1)

# -- Create S3 client
try:
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
except Exception as e:
    log_error(f"Failed to create S3 client: {str(e)}")
    sys.exit(1)

# -- Process eligible files
for row in eligible_files_cte.collect():
    file_id = row["file_id"]
    file_name = row["file_name"]
    s3_landing_path = row["s3_landing_path"]
    s3_archive_path = row["s3_archive_path"]

    # -- Validate S3 path format
    src_bucket, src_prefix = parse_s3_path(s3_landing_path)
    tgt_bucket, tgt_prefix = parse_s3_path(s3_archive_path)
    if not src_bucket or not src_prefix or not tgt_bucket or not tgt_prefix:
        log_error(f"Invalid S3 path format for file {file_name}")
        continue

    # -- Check if file exists in landing folder
    if not s3_file_exists(s3_client, s3_landing_path, file_name):
        log_error(f"File {file_name} not found in {s3_landing_path}")
        continue

    # -- Check if file already exists in archive folder
    if s3_file_exists(s3_client, s3_archive_path, file_name):
        log_error(f"File {file_name} already exists in {s3_archive_path}")
        continue

    # -- Move file from landing to archive
    moved, err = move_s3_file(s3_client, s3_landing_path, s3_archive_path, file_name)
    if moved:
        log_info(f"File {file_name} moved from {s3_landing_path} to {s3_archive_path}")
    else:
        if err and "AccessDenied" in err:
            log_error(f"Insufficient S3 permissions to move file {file_name} from {s3_landing_path} to {s3_archive_path}")
        else:
            log_error(f"Failed to move file {file_name}: {err}")

# ============================================================
# End of S3 File Migration Script
# ============================================================
# spark.stop()  # Do not stop Spark in Databricks

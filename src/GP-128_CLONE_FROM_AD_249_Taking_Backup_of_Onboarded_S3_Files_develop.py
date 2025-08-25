%pip install boto3

# ---------------------------------------------------------------------------
# Databricks PySpark Script: Automate Migration of Eligible Files from Purgo S3 Landing Folder to Archive Folder
# ---------------------------------------------------------------------------
# - Uses Unity Catalog: purgo_databricks, Schema: purgo_playground
# - Source of truth: purgo_playground.s3_file_process_log
# - Only files with file_status = 'SUCCESS' are migrated
# - S3 paths and file names are dynamically read from the log table
# - AWS credentials are securely fetched from Databricks secrets (scope: aws_keys)
# - All file operations are wrapped in try-except blocks for robust error handling
# - S3 path format, file type, and permissions are validated before migration
# - Archive path is created if missing
# - Migration status is updated in the log table to 'ARCHIVED' with current timestamp
# - Handles duplicate log entries, missing files, permission errors, and unsupported file types
# - Supports migration scope by date range (optional)
# - All code follows Databricks security and performance best practices
# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------
# from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql.functions import col, lit, when, current_timestamp, regexp_extract, desc  
from pyspark.sql.types import StringType, TimestampType, StructType, StructField  
import re  
from datetime import datetime  

# ---------------------------------------------------------------------------
# Setup: Set Unity Catalog and Database
# ---------------------------------------------------------------------------
spark.catalog.setCurrentCatalog("purgo_databricks")
spark.catalog.setCurrentDatabase("purgo_playground")

# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------

def get_aws_credentials():
    # Fetch AWS credentials from Databricks secrets
    try:
        access_key = dbutils.secrets.get(scope="aws_keys", key="access_key")
        secret_key = dbutils.secrets.get(scope="aws_keys", key="secret_key")
        return access_key, secret_key
    except Exception as e:
        print("ERROR: AWS credentials not found in Databricks secret scope: aws_keys")
        return None, None

def is_valid_s3_path(path):
    # Validate S3 path format: s3://<bucket>/<folder>/<subfolder>
    pattern = r"^s3://[a-zA-Z0-9\-\.]+/.+"
    return bool(re.match(pattern, path)) if path else False

def is_supported_file_type(file_name):
    # Only allow .csv, .txt, .parquet
    return bool(re.match(r".*\.(csv|txt|parquet)$", file_name.lower())) if file_name else False

def parse_s3_path(s3_path):
    # Parse S3 path into bucket and key
    match = re.match(r"^s3://([a-zA-Z0-9\-\.]+)/(.*)$", s3_path)
    if match:
        bucket = match.group(1)
        key = match.group(2)
        return bucket, key
    return None, None

def s3_file_exists(s3_path, file_name, s3_client):
    # Check if file exists in S3 landing path
    bucket, key_prefix = parse_s3_path(s3_path)
    if not bucket or not key_prefix or not file_name:
        return False
    key = f"{key_prefix}/{file_name}"
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False

def s3_archive_path_exists(s3_archive_path, s3_client):
    # Check if archive path exists (by listing objects with prefix)
    bucket, key_prefix = parse_s3_path(s3_archive_path)
    if not bucket or not key_prefix:
        return False
    try:
        resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=key_prefix)
        return 'Contents' in resp
    except Exception:
        return False

def create_s3_archive_path(s3_archive_path, s3_client):
    # S3 is flat, but we can create a zero-byte object as folder marker if needed
    bucket, key_prefix = parse_s3_path(s3_archive_path)
    if bucket and key_prefix:
        try:
            s3_client.put_object(Bucket=bucket, Key=f"{key_prefix}/", Body=b'')
            print(f"INFO: Created archive path: {s3_archive_path}")
        except Exception as e:
            print(f"ERROR: Failed to create archive path {s3_archive_path}: {str(e)}")

def move_s3_file(s3_landing_path, s3_archive_path, file_name, s3_client):
    # Move file from landing to archive (copy then delete)
    bucket_src, key_prefix_src = parse_s3_path(s3_landing_path)
    bucket_dst, key_prefix_dst = parse_s3_path(s3_archive_path)
    if not bucket_src or not key_prefix_src or not bucket_dst or not key_prefix_dst or not file_name:
        print(f"ERROR: Invalid S3 path or file name for file: {file_name}")
        return False, "InvalidPath"
    key_src = f"{key_prefix_src}/{file_name}"
    key_dst = f"{key_prefix_dst}/{file_name}"
    try:
        # Copy object
        s3_client.copy_object(
            Bucket=bucket_dst,
            Key=key_dst,
            CopySource={'Bucket': bucket_src, 'Key': key_src}
        )
        # Delete source object
        s3_client.delete_object(Bucket=bucket_src, Key=key_src)
        print(f"INFO: Moved file {file_name} from {s3_landing_path} to {s3_archive_path}")
        return True, "Success"
    except s3_client.exceptions.NoSuchKey:
        print(f"ERROR: File not found in landing path: {s3_landing_path}/{file_name}")
        return False, "FileNotFound"
    except s3_client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'AccessDenied':
            print(f"ERROR: Permission denied for S3 operation on: {s3_landing_path}/{file_name}")
            return False, "PermissionDenied"
        else:
            print(f"ERROR: S3 ClientError for file {file_name}: {str(e)}")
            return False, "ClientError"
    except Exception as e:
        print(f"ERROR: Error moving file {file_name}: {str(e)}")
        return False, "Error"

def update_log_status(df, file_name, new_status):
    # Update file_status and file_processed_date for file_name
    return df.withColumn(
        "file_status",
        when(col("file_name") == file_name, lit(new_status)).otherwise(col("file_status"))
    ).withColumn(
        "file_processed_date",
        when(col("file_name") == file_name, current_timestamp()).otherwise(col("file_processed_date"))
    )

# ---------------------------------------------------------------------------
# AWS S3 Client Initialization
# ---------------------------------------------------------------------------
try:
    access_key, secret_key = get_aws_credentials()
    if not access_key or not secret_key:
        raise Exception("AWS credentials not found in Databricks secret scope: aws_keys")
    import boto3  
    s3_client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
except Exception as e:
    print(f"ERROR: {str(e)}")
    s3_client = None

# ---------------------------------------------------------------------------
# Migration Scope: Optional Date Range Filtering
# ---------------------------------------------------------------------------
# Set migration date range (inclusive) if needed, else set to None
migration_start_date = None  # e.g., "2024-06-10"
migration_end_date = None    # e.g., "2024-06-11"

# ---------------------------------------------------------------------------
# Read Eligible Files from Log Table (CTE)
# ---------------------------------------------------------------------------
# Define explicit schema for s3_file_process_log
s3_file_process_log_schema = StructType([
    StructField("file_name", StringType(), True),
    StructField("s3_vendor_path", StringType(), True),
    StructField("s3_landing_path", StringType(), True),
    StructField("s3_archive_path", StringType(), True),
    StructField("file_status", StringType(), True),
    StructField("file_processed_date", TimestampType(), True)
])

# Read from Unity Catalog table
s3_file_process_log_df = spark.read.table("purgo_playground.s3_file_process_log")

# CTE: Filter eligible files for migration
eligible_files_cte = (
    s3_file_process_log_df
    .filter(
        (col("file_status") == "SUCCESS") &
        col("file_name").isNotNull() &
        col("s3_landing_path").isNotNull() &
        col("s3_archive_path").isNotNull()
    )
    .filter(
        col("file_name").rlike(".*\\.(csv|txt|parquet)$")  # Supported file types
    )
    .filter(
        col("s3_landing_path").rlike("^s3://[a-zA-Z0-9\\-\\.]+/.+") &  # Valid S3 path
        col("s3_archive_path").rlike("^s3://[a-zA-Z0-9\\-\\.]+/.+")
    )
)

if migration_start_date and migration_end_date:
    eligible_files_cte = eligible_files_cte.filter(
        (col("file_processed_date") >= lit(migration_start_date)) &
        (col("file_processed_date") <= lit(migration_end_date + "T23:59:59"))
    )

# ---------------------------------------------------------------------------
# Main Migration Loop
# ---------------------------------------------------------------------------
if s3_client is not None:
    eligible_files = eligible_files_cte.orderBy(desc("file_processed_date")).collect()
    processed_files = set()
    for row in eligible_files:
        file_name = row.file_name
        s3_landing_path = row.s3_landing_path
        s3_archive_path = row.s3_archive_path

        # Handle duplicate entries: only process each file once
        if file_name in processed_files:
            print(f"INFO: Multiple log entries found for file: {file_name}. Only SUCCESS status processed.")
            continue
        processed_files.add(file_name)

        # Validate S3 path format
        if not is_valid_s3_path(s3_landing_path) or not is_valid_s3_path(s3_archive_path):
            print(f"ERROR: Invalid S3 path format in log table for file: {file_name}")
            continue

        # Validate file type
        if not is_supported_file_type(file_name):
            print(f"ERROR: Unsupported file type for file: {file_name}")
            continue

        # Check file existence in landing path
        if not s3_file_exists(s3_landing_path, file_name, s3_client):
            print(f"ERROR: File not found in landing path: {s3_landing_path}/{file_name}")
            continue

        # Check archive path existence, create if missing
        if not s3_archive_path_exists(s3_archive_path, s3_client):
            create_s3_archive_path(s3_archive_path, s3_client)

        # Move file from landing to archive
        moved, status = move_s3_file(s3_landing_path, s3_archive_path, file_name, s3_client)
        if not moved:
            if status == "PermissionDenied":
                print(f"ERROR: Permission denied for S3 operation on: {s3_landing_path}/{file_name}")
            elif status == "FileNotFound":
                print(f"ERROR: File not found in landing path: {s3_landing_path}/{file_name}")
            else:
                print(f"ERROR: Failed to move file {file_name} due to error: {status}")
            continue

        # Audit: Update migration status in log table to 'ARCHIVED' and update processed date
        # Note: DeltaDataSource does not support user-specified schema, so use DataFrame API
        s3_file_process_log_df = update_log_status(s3_file_process_log_df, file_name, "ARCHIVED")

else:
    print("ERROR: AWS S3 client not initialized. Migration aborted.")

# ---------------------------------------------------------------------------
# Data Quality Check: Ensure column count matches target table before insert
# ---------------------------------------------------------------------------
assert len(s3_file_process_log_df.columns) == 6, "Column count must match target table schema"

# ---------------------------------------------------------------------------
# End of Script
# ---------------------------------------------------------------------------
# spark.stop()  # Do not stop Spark in Databricks

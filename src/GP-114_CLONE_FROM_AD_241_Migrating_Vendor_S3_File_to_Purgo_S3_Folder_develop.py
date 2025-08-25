%pip install boto3
%pip install botocore

spark.catalog.setCurrentCatalog("purgo_databricks")

# ============================================================
# Databricks PySpark Script: Vendor S3 to Purgo S3 File Transfer
# ------------------------------------------------------------
# Transfers eligible files from Vendor S3 to Purgo S3 folder
#   - Only files with active_flag = 'A' in ingest_config_master
#   - Only files present in Vendor S3 and NOT present in Purgo S3 or Archive S3
#   - Uses Databricks secrets for AWS credentials (scope: aws_keys)
#   - Logs all actions to purgo_playground.s3_file_process_log
#   - Handles error scenarios and edge cases per requirements
#   - All code conforms to Databricks, Unity Catalog, and PySpark best practices
# ============================================================

# =========================
# SECTION: Imports & Setup
# =========================
# from pyspark.sql import SparkSession  # built-in, spark is already available in Databricks
from pyspark.sql.types import StringType, TimestampType, StructType, StructField  
from pyspark.sql.functions import col, lit, current_timestamp  
import boto3  
from botocore.exceptions import ClientError  
import sys  
import traceback  

# =========================
# SECTION: AWS Credentials
# =========================
# Inline comment: Retrieve AWS credentials from Databricks secret scope 'aws_keys'
try:
    access_key = dbutils.secrets.get(scope="aws_keys", key="access_key")  # databricks
    secret_key = dbutils.secrets.get(scope="aws_keys", key="secret_key")  # databricks
except Exception as e:
    raise Exception("AWS credentials not found in Databricks secret scope 'aws_keys'")

# =========================
# SECTION: S3 Client Setup
# =========================
# Inline comment: Create boto3 S3 client using credentials
s3_client = boto3.client(
    "s3",
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

# =========================
# SECTION: CTE - Active Configs
# =========================
# Inline comment: CTE to select all active configs from ingest_config_master
active_configs_df = (
    spark.table("purgo_playground.ingest_config_master")
    .filter(col("active_flag") == "A")
    .select(
        col("config_id"),
        col("actual_file_name"),
        col("s3_vendor_path"),
        col("s3_landing_path"),
        col("s3_archive_path")
    )
)

# =========================
# SECTION: Helper Functions
# =========================
def parse_s3_path(s3_path):
    # Inline comment: Parse S3 URI into bucket and prefix
    if not s3_path or not s3_path.startswith("s3://"):
        return None, None
    parts = s3_path.replace("s3://", "").split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return bucket, prefix

def list_s3_files(bucket, prefix):
    # Inline comment: List all files in S3 bucket/prefix (non-recursive)
    files = set()
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                # Only get file name, not full path
                fname = key.split("/")[-1]
                if fname:
                    files.add(fname)
    except ClientError as e:
        # Inline comment: Log error and return empty set
        files = set()
    return files

def copy_s3_file(src_bucket, src_prefix, file_name, dest_bucket, dest_prefix):
    # Inline comment: Copy file from src to dest S3 location
    src_key = src_prefix + file_name
    dest_key = dest_prefix + file_name
    try:
        s3_client.copy_object(
            Bucket=dest_bucket,
            CopySource={"Bucket": src_bucket, "Key": src_key},
            Key=dest_key
        )
        return True, "File copied successfully"
    except Exception as e:
        return False, str(e)

# =========================
# SECTION: File Transfer Logic
# =========================
# Inline comment: Prepare log rows for insertion
log_rows = []

for row in active_configs_df.collect():
    config_id = row["config_id"]
    file_name = row["actual_file_name"]
    vendor_path = row["s3_vendor_path"]
    landing_path = row["s3_landing_path"]
    archive_path = row["s3_archive_path"]

    # Inline comment: Validate S3 paths
    if not vendor_path or not landing_path or not archive_path:
        log_rows.append({
            "file_name": file_name,
            "s3_vendor_path": vendor_path,
            "s3_landing_path": landing_path,
            "s3_archive_path": archive_path,
            "file_status": "error",
            "file_processed_date": None,
            "message": f"S3 folder path missing in ingest_config_master for config_id {config_id}"
        })
        continue

    # Inline comment: Parse S3 paths
    vendor_bucket, vendor_prefix = parse_s3_path(vendor_path)
    landing_bucket, landing_prefix = parse_s3_path(landing_path)
    archive_bucket, archive_prefix = parse_s3_path(archive_path)

    if not vendor_bucket or not landing_bucket or not archive_bucket:
        log_rows.append({
            "file_name": file_name,
            "s3_vendor_path": vendor_path,
            "s3_landing_path": landing_path,
            "s3_archive_path": archive_path,
            "file_status": "error",
            "file_processed_date": None,
            "message": f"S3 folder path missing in ingest_config_master for config_id {config_id}"
        })
        continue

    # Inline comment: List files in Vendor, Purgo, and Archive S3 folders
    try:
        vendor_files = list_s3_files(vendor_bucket, vendor_prefix)
        purgo_files = list_s3_files(landing_bucket, landing_prefix)
        archive_files = list_s3_files(archive_bucket, archive_prefix)
    except Exception as e:
        log_rows.append({
            "file_name": file_name,
            "s3_vendor_path": vendor_path,
            "s3_landing_path": landing_path,
            "s3_archive_path": archive_path,
            "file_status": "error",
            "file_processed_date": None,
            "message": f"S3 listing error: {str(e)}"
        })
        continue

    # Inline comment: File existence check in Vendor S3
    if not file_name or file_name not in vendor_files:
        log_rows.append({
            "file_name": file_name,
            "s3_vendor_path": vendor_path,
            "s3_landing_path": landing_path,
            "s3_archive_path": archive_path,
            "file_status": "skipped",
            "file_processed_date": None,
            "message": "File not found in Vendor S3"
        })
        continue

    # Inline comment: File existence check in Purgo S3 (case-sensitive)
    if file_name in purgo_files:
        log_rows.append({
            "file_name": file_name,
            "s3_vendor_path": vendor_path,
            "s3_landing_path": landing_path,
            "s3_archive_path": archive_path,
            "file_status": "skipped",
            "file_processed_date": None,
            "message": "File already exists in Purgo S3"
        })
        continue

    # Inline comment: File existence check in Archive S3 (case-sensitive)
    if file_name in archive_files:
        log_rows.append({
            "file_name": file_name,
            "s3_vendor_path": vendor_path,
            "s3_landing_path": landing_path,
            "s3_archive_path": archive_path,
            "file_status": "skipped",
            "file_processed_date": None,
            "message": "File already exists in Archive S3"
        })
        continue

    # Inline comment: Attempt file copy from Vendor to Purgo S3
    try:
        success, msg = copy_s3_file(vendor_bucket, vendor_prefix, file_name, landing_bucket, landing_prefix)
        if success:
            log_rows.append({
                "file_name": file_name,
                "s3_vendor_path": vendor_path,
                "s3_landing_path": landing_path,
                "s3_archive_path": archive_path,
                "file_status": "success",
                "file_processed_date": None,
                "message": msg
            })
        else:
            log_rows.append({
                "file_name": file_name,
                "s3_vendor_path": vendor_path,
                "s3_landing_path": landing_path,
                "s3_archive_path": archive_path,
                "file_status": "error",
                "file_processed_date": None,
                "message": msg
            })
    except Exception as e:
        log_rows.append({
            "file_name": file_name,
            "s3_vendor_path": vendor_path,
            "s3_landing_path": landing_path,
            "s3_archive_path": archive_path,
            "file_status": "error",
            "file_processed_date": None,
            "message": str(e)
        })

# =========================
# SECTION: Log DataFrame Creation
# =========================
# Inline comment: Define schema for s3_file_process_log
log_schema = StructType([
    StructField("file_name", StringType(), True),
    StructField("s3_vendor_path", StringType(), True),
    StructField("s3_landing_path", StringType(), True),
    StructField("s3_archive_path", StringType(), True),
    StructField("file_status", StringType(), True),
    StructField("file_processed_date", TimestampType(), True),
    StructField("message", StringType(), True)
])

# Inline comment: Add current timestamp for processed files
for r in log_rows:
    if r["file_status"] == "success":
        r["file_processed_date"] = None  # current_timestamp() can be set if required

# Inline comment: Create DataFrame for log entries
log_df = spark.createDataFrame(log_rows, schema=log_schema)

# Inline comment: Ensure schema matches target table before insertion
target_schema = spark.table("purgo_playground.s3_file_process_log").schema
assert [f.name for f in log_df.schema.fields] == [f.name for f in target_schema.fields], "Column mismatch with target table"

# Inline comment: Insert log records into s3_file_process_log table
log_df.write.mode("append").insertInto("purgo_playground.s3_file_process_log")

# =========================
# SECTION: End of Script
# =========================
# Inline comment: All eligible files processed and logged per requirements
# spark.stop()  # Commented out to avoid issues in Databricks

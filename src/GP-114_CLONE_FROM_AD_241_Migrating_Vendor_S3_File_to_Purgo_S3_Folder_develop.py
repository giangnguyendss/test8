# ============================================================
# Databricks PySpark Script: Transfer Eligible Vendor S3 Files to Purgo S3 Folder
# ============================================================
# Description:
#   - Transfers files from Vendor S3 bucket to Purgo S3 folder based on config in purgo_playground.ingest_config_master
#   - Only files with active_flag = 'A' are considered
#   - Excludes files already present in Purgo or Archive S3 folders
#   - Handles recursive transfer, file deletion, file name pattern, file format, and error scenarios
#   - Logs all transfer attempts to purgo_playground.s3_file_process_log
#   - Uses Databricks secrets for AWS credentials
#   - All code follows Databricks best practices and Unity Catalog conventions
#   - All comments are inline or header as required
# ============================================================

# -- SparkSession is already available in Databricks
# from pyspark.sql import SparkSession

from pyspark.sql.functions import col, lit, current_timestamp, regexp_extract
from pyspark.sql.types import StringType, TimestampType
import re

# ============================================================
# Section: Helper Functions
# ============================================================

def get_aws_credentials():
    # -- Retrieve AWS credentials from Databricks secret scope
    try:
        access_key = dbutils.secrets.get(scope="aws_keys", key="access_key")
    except Exception:
        raise Exception("Missing AWS credential: access_key")
    try:
        secret_key = dbutils.secrets.get(scope="aws_keys", key="secret_key")
    except Exception:
        raise Exception("Missing AWS credential: secret_key")
    return access_key, secret_key

def list_s3_files(s3_path, recursive=False):
    # -- List files in S3 path using Databricks utilities
    # -- Returns list of file names (not full paths)
    try:
        files = []
        if recursive:
            # -- Recursively list all files under s3_path
            for entry in dbutils.fs.ls(s3_path):
                if entry.isDir():
                    files += list_s3_files(entry.path, recursive=True)
                else:
                    files.append(entry.name)
        else:
            files = [entry.name for entry in dbutils.fs.ls(s3_path) if not entry.isDir()]
        return files
    except Exception as e:
        raise Exception(f"S3 access denied for path: {s3_path}")

def file_exists_in_s3(s3_path, file_name, recursive=False):
    # -- Check if file exists in S3 path (by file name)
    try:
        files = list_s3_files(s3_path, recursive)
        return file_name in files
    except Exception:
        return False

def transfer_file_s3(src_path, dest_path, file_name):
    # -- Transfer file from src_path to dest_path using Databricks utilities
    src_file = src_path.rstrip("/") + "/" + file_name
    dest_file = dest_path.rstrip("/") + "/" + file_name
    try:
        dbutils.fs.cp(src_file, dest_file)
        return True
    except Exception as e:
        return False

def delete_file_s3(s3_path, file_name):
    # -- Delete file from S3 path using Databricks utilities
    file_path = s3_path.rstrip("/") + "/" + file_name
    try:
        dbutils.fs.rm(file_path)
        return True
    except Exception as e:
        return False

def validate_file_name_pattern(file_name, pattern):
    # -- Validate file name against regex pattern
    if file_name is None or file_name == "":
        return False
    return re.fullmatch(pattern, file_name) is not None

def validate_file_format(file_name, allowed_exts):
    # -- Validate file format by extension
    if file_name is None or file_name == "":
        return False
    return any(file_name.lower().endswith(ext.lower()) for ext in allowed_exts)

def log_file_process(file_name, s3_vendor_path, s3_landing_path, s3_archive_path, file_status, reason=None):
    # -- Log file transfer attempt to s3_file_process_log
    from datetime import datetime
    from pyspark.sql import Row
    log_row = Row(
        file_name=file_name,
        s3_vendor_path=s3_vendor_path,
        s3_landing_path=s3_landing_path,
        s3_archive_path=s3_archive_path,
        file_status=file_status if reason is None else f"{file_status}: {reason}",
        file_processed_date=datetime.utcnow()
    )
    log_df = spark.createDataFrame([log_row])
    # -- Ensure schema matches target table
    log_df = log_df.select(
        col("file_name").cast(StringType()),
        col("s3_vendor_path").cast(StringType()),
        col("s3_landing_path").cast(StringType()),
        col("s3_archive_path").cast(StringType()),
        col("file_status").cast(StringType()),
        col("file_processed_date").cast(TimestampType())
    )
    log_df.write.mode("append").saveAsTable("purgo_playground.s3_file_process_log")

# ============================================================
# Section: Main Processing Logic
# ============================================================

def main():
    # -- Set Unity Catalog and Schema
    spark.catalog.setCurrentCatalog("purgo_databricks")
    spark.catalog.setCurrentDatabase("purgo_playground")

    # -- Retrieve AWS credentials
    try:
        access_key, secret_key = get_aws_credentials()
    except Exception as cred_err:
        # -- Log error and exit
        log_file_process(None, None, None, None, "FAILED", str(cred_err))
        return

    # -- Allowed file extensions and file name pattern (can be parameterized)
    allowed_file_exts = [".csv", ".txt"]
    file_name_pattern = r"^[a-zA-Z0-9_\-\.]+$"  # -- Default: alphanum, _, -, . (can be config driven)

    # -- Read ingest_config_master table
    config_df = spark.table("purgo_playground.ingest_config_master")

    # -- CTE: Select eligible configs
    eligible_configs = config_df.where(
        (col("active_flag") == "A") &
        (col("file_name").isNotNull()) &
        (col("file_name") != "")
    ).select(
        "config_id", "file_name", "s3_vendor_path", "s3_landing_path", "s3_archive_path",
        "vendor_file_deletion_flag", "file_recursive_flag", "date_pattern"
    )

    # -- Track if any eligible files found
    eligible_file_found = False

    # -- Process each eligible config row
    for row in eligible_configs.collect():
        config_id = row["config_id"]
        file_name = row["file_name"]
        s3_vendor_path = row["s3_vendor_path"]
        s3_landing_path = row["s3_landing_path"]
        s3_archive_path = row["s3_archive_path"]
        vendor_file_deletion_flag = (row["vendor_file_deletion_flag"] or "").upper()
        file_recursive_flag = (row["file_recursive_flag"] or "").upper()
        date_pattern = row["date_pattern"]

        # -- Validate S3 paths
        missing_column = None
        if not s3_vendor_path or s3_vendor_path.strip() == "":
            missing_column = "s3_vendor_path"
        elif not s3_landing_path or s3_landing_path.strip() == "":
            missing_column = "s3_landing_path"
        elif not s3_archive_path or s3_archive_path.strip() == "":
            missing_column = "s3_archive_path"
        if missing_column:
            log_file_process(file_name, s3_vendor_path, s3_landing_path, s3_archive_path, "FAILED", f"Missing S3 folder path: {missing_column}")
            continue

        # -- Validate file name pattern if date_pattern or pattern is specified
        # -- For demo, use file_name_pattern; in real, use date_pattern if provided
        if not validate_file_name_pattern(file_name, file_name_pattern):
            log_file_process(file_name, s3_vendor_path, s3_landing_path, s3_archive_path, "FAILED", f"Invalid file name format: {file_name}")
            continue

        # -- Validate file format
        if not validate_file_format(file_name, allowed_file_exts):
            log_file_process(file_name, s3_vendor_path, s3_landing_path, s3_archive_path, "SKIPPED", "File format not allowed")
            continue

        # -- List files in Vendor S3 folder (recursive if flag is Y)
        recursive = file_recursive_flag == "Y"
        try:
            vendor_files = list_s3_files(s3_vendor_path, recursive)
        except Exception as e:
            log_file_process(file_name, s3_vendor_path, s3_landing_path, s3_archive_path, "FAILED", str(e))
            continue

        # -- Check if file exists in Vendor S3 folder
        if file_name not in vendor_files:
            log_file_process(file_name, s3_vendor_path, s3_landing_path, s3_archive_path, "FAILED", "File not found in Vendor S3")
            continue

        # -- Check if file exists in Purgo S3 folder
        try:
            purgo_exists = file_exists_in_s3(s3_landing_path, file_name, recursive)
        except Exception as e:
            log_file_process(file_name, s3_vendor_path, s3_landing_path, s3_archive_path, "FAILED", str(e))
            continue
        if purgo_exists:
            log_file_process(file_name, s3_vendor_path, s3_landing_path, s3_archive_path, "SKIPPED", "File already exists in Purgo S3")
            continue

        # -- Check if file exists in Archive S3 folder
        try:
            archive_exists = file_exists_in_s3(s3_archive_path, file_name, recursive)
        except Exception as e:
            log_file_process(file_name, s3_vendor_path, s3_landing_path, s3_archive_path, "FAILED", str(e))
            continue
        if archive_exists:
            log_file_process(file_name, s3_vendor_path, s3_landing_path, s3_archive_path, "SKIPPED", "File already exists in Archive S3")
            continue

        # -- Eligible file found
        eligible_file_found = True

        # -- Transfer file from Vendor S3 to Purgo S3
        transfer_success = transfer_file_s3(s3_vendor_path, s3_landing_path, file_name)
        if not transfer_success:
            log_file_process(file_name, s3_vendor_path, s3_landing_path, s3_archive_path, "FAILED", "File transfer failed")
            continue

        # -- If vendor_file_deletion_flag = 'Y', delete file from Vendor S3
        if vendor_file_deletion_flag == "Y":
            delete_success = delete_file_s3(s3_vendor_path, file_name)
            if not delete_success:
                log_file_process(file_name, s3_vendor_path, s3_landing_path, s3_archive_path, "FAILED", "File deletion from Vendor S3 failed")
                continue

        # -- Log successful transfer
        log_file_process(file_name, s3_vendor_path, s3_landing_path, s3_archive_path, "SUCCESS")

    # -- If no eligible files found, log NO FILES TO TRANSFER
    if not eligible_file_found:
        log_file_process(None, None, None, None, "NO FILES TO TRANSFER")

# ============================================================
# Section: Entry Point
# ============================================================

if __name__ == "__main__":
    main()
# ============================================================
# End of Script
# ============================================================

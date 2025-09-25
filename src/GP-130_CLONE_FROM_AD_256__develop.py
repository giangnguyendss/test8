spark.catalog.setCurrentCatalog("purgo_databricks")

# Databricks PySpark: Real-time streaming of patient data from Purgo S3 to patient_data_auto_loader table
# Catalog: purgo_databricks
# Schema: purgo_playground
# Table: patient_data_auto_loader
# S3 Autoloader Streaming from dynamic path in ingest_config_master
# All columns as String except data_loaded_at (Timestamp)
# Checkpoint: /mnt/checkpoints/patient_al_cp/
# Schema location: /mnt/checkpoints/s3_autoloader/patient_al_schema
# S3 credentials from secret scope: aws_keys

# ---------------------------
# Imports
# ---------------------------
from pyspark.sql import functions as F  
from pyspark.sql.types import StructType, StructField, StringType, TimestampType  

# ---------------------------
# Securely retrieve S3 credentials from Databricks secret scope
# ---------------------------
try:
    access_key = dbutils.secrets.get(scope="aws_keys", key="access_key")  # databricks-utils
    secret_key = dbutils.secrets.get(scope="aws_keys", key="secret_key")  # databricks-utils
except Exception as e:
    # Log error and exit if credentials are missing
    print(f"Missing or invalid S3 credentials: {str(e)}")
    raise

# ---------------------------
# Configure Spark for S3 access
# ---------------------------
spark.conf.set("fs.s3a.access.key", access_key)
spark.conf.set("fs.s3a.secret.key", secret_key)
spark.conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark.conf.set("fs.s3a.path.style.access", "true")

# ---------------------------
# Retrieve dynamic S3 path from ingest_config_master table
# ---------------------------
try:
    ingest_config_df = spark.table("purgo_databricks.purgo_playground.ingest_config_master")
    s3_path_row = (
        ingest_config_df
        .filter(F.col("source_object_name").contains("patient_data"))
        .select("s3_landing_path")
        .first()
    )
    if s3_path_row is None or not s3_path_row["s3_landing_path"]:
        raise ValueError("S3 path not found for patient_data source")
    s3_landing_path = s3_path_row["s3_landing_path"]
except Exception as e:
    # Log error and exit if S3 path is missing or inaccessible
    print(f"S3 path retrieval error: {str(e)}")
    raise

# ---------------------------
# Define CSV schema: all columns as StringType
# ---------------------------
patient_schema = StructType([
    StructField("patient_id", StringType(), True),
    StructField("patient_name", StringType(), True),
    StructField("age", StringType(), True),
    StructField("diagnosis", StringType(), True),
    StructField("treatment", StringType(), True)
])

# ---------------------------
# Ensure target table exists with correct schema
# ---------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS purgo_databricks.purgo_playground.patient_data_auto_loader (
    patient_id STRING,
    patient_name STRING,
    age STRING,
    diagnosis STRING,
    treatment STRING,
    data_loaded_at TIMESTAMP
)
""")

# ---------------------------
# Streaming ingestion from S3 using Autoloader
# ---------------------------
try:
    # Read CSV files as stream with Autoloader
    stream_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/mnt/checkpoints/s3_autoloader/patient_al_schema")
        .option("header", "true")
        .schema(patient_schema)
        .load(s3_landing_path)
    )
except Exception as e:
    # Log error and exit if S3 path is inaccessible or schema error
    print(f"Unable to access S3 path or schema error: {str(e)}")
    raise

# ---------------------------
# Data transformation: Add data_loaded_at as current timestamp
# ---------------------------
stream_df = stream_df.withColumn("data_loaded_at", F.current_timestamp())

# ---------------------------
# Data quality checks: Validate required columns and types
# ---------------------------
required_columns = ["patient_id", "patient_name", "age", "diagnosis", "treatment"]
missing_columns = [col for col in required_columns if col not in stream_df.columns]
if missing_columns:
    raise ValueError(f"Missing required column(s): {', '.join(missing_columns)}")

# Cast all columns to StringType and enforce column order
stream_df = (
    stream_df
    .select(
        F.col("patient_id").cast(StringType()),
        F.col("patient_name").cast(StringType()),
        F.col("age").cast(StringType()),
        F.col("diagnosis").cast(StringType()),
        F.col("treatment").cast(StringType()),
        F.col("data_loaded_at").cast(TimestampType())
    )
)

# ---------------------------
# Append streaming data to target table with checkpointing
# ---------------------------
(
    stream_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/checkpoints/patient_al_cp/")
    .toTable("purgo_databricks.purgo_playground.patient_data_auto_loader")
)

# End of streaming ingestion script
# spark.stop()  # Do not stop SparkSession in Databricks

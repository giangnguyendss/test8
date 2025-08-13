spark.catalog.setCurrentCatalog("purgo_databricks")

# /* 
# Databricks PySpark Implementation: Channel-wise Biosimilar Sales Performance Analysis
# 
# This script reads from purgo_playground.bai_sales_agg_obu_customer_datapack,
# filters for kanjinti, mvasi, riabni brands, validates cdl_effective_date (ISO 8601),
# filters records within 36 months from cdl_effective_date (using transaction_timestamp),
# validates competitor_flag and channel_name, logs errors to
# purgo_playground.bai_sales_agg_obu_customer_datapack_invalid_cdl_effective_date_log,
# aggregates sales metrics by channel and key attributes, and outputs grouped results as CSV.
# */

# from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql import functions as F  
from pyspark.sql.types import (StructType, StructField, StringType, BooleanType, TimestampType, LongType)  
from pyspark.sql.utils import AnalysisException  

# /* ------------------ Section: Setup and Schema Validation ------------------ */

# Define expected schema for purgo_playground.bai_sales_agg_obu_customer_datapack
expected_schema = StructType([
    StructField("brand_normalized_name", StringType(), True),
    StructField("normalized_name", StringType(), True),
    StructField("market_normalized_name", StringType(), True),
    StructField("competitor_flag", BooleanType(), True),
    StructField("channel_name", StringType(), True),
    StructField("transaction_timestamp", TimestampType(), True),
    StructField("integrated_units", LongType(), True),
    StructField("integrated_normalized_units", LongType(), True),
    StructField("integrated_dollars", LongType(), True),
    StructField("cdl_effective_date", StringType(), True)
])

# /* Validate schema of source table */
try:
    df_source = spark.table("purgo_playground.bai_sales_agg_obu_customer_datapack")
except AnalysisException as e:
    raise AssertionError(f"Source table not found: {e}")

assert df_source.schema == expected_schema, "Schema mismatch in source table"

# /* ------------------ Section: Data Quality Validation ------------------ */

# Allowed brands and channels
allowed_brands = ["kanjinti", "mvasi", "riabni"]
allowed_channels = ["online", "retail", "mobile"]

# UDF to validate ISO 8601 date format (YYYY-MM-DD)
@F.udf(StringType())
def validate_cdl_effective_date(date_str):
    import re
    if date_str is None:
        return "cdl_effective_date is null"
    if date_str == "":
        return "cdl_effective_date is empty"
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        return "cdl_effective_date format invalid"
    try:
        from datetime import datetime
        datetime.strptime(date_str, "%Y-%m-%d")
    except Exception:
        return "cdl_effective_date format invalid"
    return None

# UDF to validate competitor_flag is boolean
@F.udf(StringType())
def validate_competitor_flag(flag):
    if flag is None:
        return None
    if isinstance(flag, bool):
        return None
    return "competitor_flag not boolean"

# UDF to validate channel_name
@F.udf(StringType())
def validate_channel_name(channel):
    if channel is None:
        return "channel_name is null"
    if channel not in allowed_channels:
        return "channel_name not in allowed set"
    return None

# /* Apply validation UDFs and collect error rows */
df_validated = df_source.withColumn(
    "cdl_effective_date_error", validate_cdl_effective_date(F.col("cdl_effective_date"))
).withColumn(
    "competitor_flag_error", validate_competitor_flag(F.col("competitor_flag"))
).withColumn(
    "channel_name_error", validate_channel_name(F.col("channel_name"))
)

df_error = df_validated.filter(
    (F.col("cdl_effective_date_error").isNotNull()) |
    (F.col("competitor_flag_error").isNotNull()) |
    (F.col("channel_name_error").isNotNull())
).withColumn(
    "error_message",
    F.coalesce(
        F.col("cdl_effective_date_error"),
        F.col("competitor_flag_error"),
        F.col("channel_name_error")
    )
)

# /* Write error rows to error log table (Delta Lake append) */
error_log_table = "purgo_playground.bai_sales_agg_obu_customer_datapack_invalid_cdl_effective_date_log"
try:
    df_error.select(
        "brand_normalized_name", "normalized_name", "market_normalized_name",
        "competitor_flag", "channel_name", "transaction_timestamp",
        "integrated_units", "integrated_normalized_units", "integrated_dollars",
        "cdl_effective_date", "error_message"
    ).write.format("delta").mode("append").saveAsTable(error_log_table)
except Exception as e:
    # If error log table does not exist, create it
    df_error.select(
        "brand_normalized_name", "normalized_name", "market_normalized_name",
        "competitor_flag", "channel_name", "transaction_timestamp",
        "integrated_units", "integrated_normalized_units", "integrated_dollars",
        "cdl_effective_date", "error_message"
    ).write.format("delta").mode("overwrite").saveAsTable(error_log_table)

# /* Filter out error rows from main DataFrame */
df_clean = df_validated.filter(
    (F.col("cdl_effective_date_error").isNull()) &
    (F.col("competitor_flag_error").isNull()) &
    (F.col("channel_name_error").isNull())
)

# /* ------------------ Section: Time Window Filtering ------------------ */

# Only include allowed brands
df_clean = df_clean.filter(F.col("brand_normalized_name").isin(allowed_brands))

# Parse cdl_effective_date to date
df_clean = df_clean.withColumn(
    "cdl_effective_date_parsed", F.to_date(F.col("cdl_effective_date"), "yyyy-MM-dd")
)

# Filter for valid cdl_effective_date
df_clean = df_clean.filter(F.col("cdl_effective_date_parsed").isNotNull())

# Filter transaction_timestamp within 36 months from cdl_effective_date
df_clean = df_clean.withColumn(
    "window_start", F.col("cdl_effective_date_parsed").cast(TimestampType())
).withColumn(
    "window_end", F.expr("window_start + INTERVAL 36 MONTHS")
)

df_clean = df_clean.filter(
    (F.col("transaction_timestamp") >= F.col("window_start")) &
    (F.col("transaction_timestamp") < F.col("window_end"))
)

# /* Log error for records outside 36-month window */
df_outside_window = df_validated.withColumn(
    "cdl_effective_date_parsed", F.to_date(F.col("cdl_effective_date"), "yyyy-MM-dd")
).withColumn(
    "window_start", F.col("cdl_effective_date_parsed").cast(TimestampType())
).withColumn(
    "window_end", F.expr("window_start + INTERVAL 36 MONTHS")
).filter(
    (F.col("cdl_effective_date_parsed").isNotNull()) &
    (
        (F.col("transaction_timestamp") < F.col("window_start")) |
        (F.col("transaction_timestamp") >= F.col("window_end"))
    )
).withColumn(
    "error_message", F.lit("transaction_timestamp outside 36-month window")
)

try:
    df_outside_window.select(
        "brand_normalized_name", "normalized_name", "market_normalized_name",
        "competitor_flag", "channel_name", "transaction_timestamp",
        "integrated_units", "integrated_normalized_units", "integrated_dollars",
        "cdl_effective_date", "error_message"
    ).write.format("delta").mode("append").saveAsTable(error_log_table)
except Exception as e:
    pass  # Error log table already created above

# /* ------------------ Section: Aggregation and Output ------------------ */

# Group and aggregate sales metrics
df_agg = df_clean.groupBy(
    "brand_normalized_name",
    "normalized_name",
    "market_normalized_name",
    "competitor_flag",
    "channel_name"
).agg(
    F.sum("integrated_units").alias("sum_integrated_units"),
    F.sum("integrated_normalized_units").alias("sum_integrated_normalized_units"),
    F.sum("integrated_dollars").alias("sum_integrated_dollars")
)

# /* Assert output schema matches expected */
expected_output_schema = StructType([
    StructField("brand_normalized_name", StringType(), True),
    StructField("normalized_name", StringType(), True),
    StructField("market_normalized_name", StringType(), True),
    StructField("competitor_flag", BooleanType(), True),
    StructField("channel_name", StringType(), True),
    StructField("sum_integrated_units", LongType(), True),
    StructField("sum_integrated_normalized_units", LongType(), True),
    StructField("sum_integrated_dollars", LongType(), True)
])
assert df_agg.schema == expected_output_schema, "Output schema mismatch"

# /* Assert no column mismatch before writing to target table */
assert len(df_agg.columns) == len(expected_output_schema), "Column count mismatch in output"

# /* Output as CSV (to catalog volume, not dbfs) */
output_path = "volumes/purgo_playground/channel_sales_output.csv"
try:
    df_agg.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
except Exception as e:
    raise AssertionError(f"Failed to write output CSV: {e}")

# /* End of Implementation */

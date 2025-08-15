# ------------------------------------------------------------------------------------
# Biosimilar Brand Sales Channel-wise Aggregation for Databricks PySpark
# ------------------------------------------------------------------------------------
# This script reads purgo_playground.bai_sales_agg_obu_customer_datapack,
# filters for kanjinti, mvasi, riabni brands, validates cdl_effective_date,
# applies a 36-month window filter, aggregates sales metrics by channel and key attributes,
# logs invalid cdl_effective_date records, and outputs the result as a DataFrame.
# ------------------------------------------------------------------------------------

# spark.catalog.setCurrentCatalog("purgo_databricks")  # Catalog is already set in Databricks

# Required imports for PySpark operations
from pyspark.sql import functions as F  # Built-in
from pyspark.sql.types import LongType  # Built-in
from pyspark.sql.utils import AnalysisException  # Built-in

# ------------------------------------------------------------------------------------
# Section: Read Source Table
# ------------------------------------------------------------------------------------
try:
    df_src = spark.read.table("purgo_playground.bai_sales_agg_obu_customer_datapack")
except AnalysisException as e:
    # If table does not exist, create empty DataFrame with expected schema
    from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, LongType
    schema = StructType([
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
    df_src = spark.createDataFrame([], schema=schema)

# ------------------------------------------------------------------------------------
# Section: Data Quality - NULL Handling for Sales Metrics
# ------------------------------------------------------------------------------------
# Treat NULLs in sales metrics as zero for aggregation
df_src = df_src.withColumn("integrated_units", F.coalesce(F.col("integrated_units"), F.lit(0).cast(LongType())))
df_src = df_src.withColumn("integrated_normalized_units", F.coalesce(F.col("integrated_normalized_units"), F.lit(0).cast(LongType())))
df_src = df_src.withColumn("integrated_dollars", F.coalesce(F.col("integrated_dollars"), F.lit(0).cast(LongType()))

# ------------------------------------------------------------------------------------
# Section: Log Invalid or Null cdl_effective_date Records
# ------------------------------------------------------------------------------------
df_invalid_cdl = df_src.filter(
    (F.col("cdl_effective_date").isNull()) |
    (~F.col("cdl_effective_date").rlike(r"^\d{4}-\d{2}-\d{2}$"))
).withColumn(
    "error_message",
    F.when(F.col("cdl_effective_date").isNull(), F.lit("cdl_effective_date is null"))
     .otherwise(F.lit("cdl_effective_date format invalid"))
)

# Write invalid records to log table (Delta Lake)
df_invalid_cdl.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("purgo_playground.bai_sales_agg_obu_customer_datapack_invalid_cdl_effective_date_log")

# ------------------------------------------------------------------------------------
# Section: Filter Valid Records for Analysis
# ------------------------------------------------------------------------------------
valid_brands = ["kanjinti", "mvasi", "riabni"]
df_valid = df_src.filter(
    (F.col("cdl_effective_date").isNotNull()) &
    (F.col("cdl_effective_date").rlike(r"^\d{4}-\d{2}-\d{2}$")) &
    (F.col("brand_normalized_name").isin(valid_brands))
)

# Convert cdl_effective_date to timestamp
df_valid = df_valid.withColumn("cdl_effective_date_ts", F.to_timestamp("cdl_effective_date", "yyyy-MM-dd"))

# Filter transaction_timestamp within 36 months from cdl_effective_date (inclusive)
df_valid = df_valid.withColumn(
    "txn_within_36_months",
    (F.col("transaction_timestamp") >= F.col("cdl_effective_date_ts")) &
    (F.col("transaction_timestamp") <= F.expr("add_months(cdl_effective_date_ts, 36)"))
)
df_valid = df_valid.filter(F.col("txn_within_36_months"))

# ------------------------------------------------------------------------------------
# Section: Aggregation - Channel-wise Grouping
# ------------------------------------------------------------------------------------
df_agg = df_valid.groupBy(
    "brand_normalized_name", "normalized_name", "market_normalized_name",
    "competitor_flag", "channel_name"
).agg(
    F.sum("integrated_units").alias("total_integrated_units"),
    F.sum("integrated_normalized_units").alias("total_integrated_normalized_units"),
    F.sum("integrated_dollars").alias("total_integrated_dollars")
)

# ------------------------------------------------------------------------------------
# Section: Output - Show Result
# ------------------------------------------------------------------------------------
df_agg.show(truncate=False)

# ------------------------------------------------------------------------------------
# Optionally write output to channelwise staging table (Delta Lake)
df_agg.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("purgo_playground.bai_sales_agg_obu_customer_datapack_channelwise_stg")

# ------------------------------------------------------------------------------------
# End of Biosimilar Brand Sales Channel-wise Aggregation Script
# ------------------------------------------------------------------------------------
# spark.stop()  # Do not stop SparkSession in Databricks


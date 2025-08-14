# ------------------------------------------------------------------------------------
# Channel-wise Sales Performance Analysis for Biosimilar Brands (Kanjinti, Mvasi, Riabni)
# Aggregates sales metrics by channel within 36 months of cdl_effective_date,
# handles invalid/null cdl_effective_date, deduplication, and writes results to staging table.
# ------------------------------------------------------------------------------------
# Assumptions:
# - spark session is available
# - Source table: purgo_playground.bai_sales_agg_obu_customer_datapack
# - Output table: purgo_playground.bai_sales_agg_obu_customer_datapack_channelwise_stg
# - Error log table: purgo_playground.bai_sales_agg_obu_customer_datapack_invalid_cdl_effective_date_log
# ------------------------------------------------------------------------------------

# from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql import functions as F  
from pyspark.sql.types import StringType, BooleanType, TimestampType, LongType  
from pyspark.sql.types import StructType, StructField  

# -- Set current catalog for Unity Catalog
spark.catalog.setCurrentCatalog("purgo_databricks")

# ------------------------------------------------------------------------------------
# Section: Read Source Data
# ------------------------------------------------------------------------------------
df = spark.table("purgo_playground.bai_sales_agg_obu_customer_datapack")

# ------------------------------------------------------------------------------------
# Section: Validate cdl_effective_date Format
# ------------------------------------------------------------------------------------
# Only accept cdl_effective_date in 'yyyy-MM-dd' format (exclude nulls and invalids)
import re  

def is_valid_date(date_str):
    if date_str is None:
        return False
    return bool(re.match(r"^\d{4}-\d{2}-\d{2}$", date_str))

from pyspark.sql.functions import udf  
valid_cdl_effective_date_udf = udf(is_valid_date, BooleanType())

df_valid = df.filter(
    (F.col("cdl_effective_date").isNotNull()) &
    (valid_cdl_effective_date_udf(F.col("cdl_effective_date")))
)

df_invalid = df.filter(
    (F.col("cdl_effective_date").isNull()) |
    (~valid_cdl_effective_date_udf(F.col("cdl_effective_date")))
)

# ------------------------------------------------------------------------------------
# Section: Log Invalid cdl_effective_date Records
# ------------------------------------------------------------------------------------
df_invalid_log = df_invalid.withColumn("error_message", F.lit("Invalid or null cdl_effective_date"))
df_invalid_log.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "purgo_playground.bai_sales_agg_obu_customer_datapack_invalid_cdl_effective_date_log"
)

# ------------------------------------------------------------------------------------
# Section: Filter for Biosimilar Brands and Valid Channels
# ------------------------------------------------------------------------------------
biosimilar_brands = ["Kanjinti", "Mvasi", "Riabni"]
valid_channels = ["online", "retail", "mobile"]

df_filtered = df_valid.filter(
    (F.col("brand_normalized_name").isin(biosimilar_brands)) &
    (F.col("channel_name").isin(valid_channels))
)

# ------------------------------------------------------------------------------------
# Section: Deduplicate Records
# ------------------------------------------------------------------------------------
# Remove duplicates based on all grouping keys and transaction_timestamp
df_dedup = df_filtered.dropDuplicates([
    "brand_normalized_name", "normalized_name", "market_normalized_name",
    "competitor_flag", "channel_name", "transaction_timestamp"
])

# ------------------------------------------------------------------------------------
# Section: Time Window Filtering (36 months from cdl_effective_date)
# ------------------------------------------------------------------------------------
df_dedup = df_dedup.withColumn(
    "cdl_effective_date_dt", F.to_date("cdl_effective_date", "yyyy-MM-dd")
)
df_dedup = df_dedup.withColumn(
    "window_start", F.col("cdl_effective_date_dt")
).withColumn(
    "window_end", F.add_months(F.col("cdl_effective_date_dt"), 36)
)
df_windowed = df_dedup.filter(
    (F.col("transaction_timestamp") >= F.col("window_start")) &
    (F.col("transaction_timestamp") < F.col("window_end"))
)

# ------------------------------------------------------------------------------------
# Section: NULL Sales Metrics Handling
# ------------------------------------------------------------------------------------
# Treat NULL sales metrics as zero for aggregation
df_windowed = df_windowed.withColumn(
    "integrated_units", F.coalesce(F.col("integrated_units"), F.lit(0))
).withColumn(
    "integrated_normalized_units", F.coalesce(F.col("integrated_normalized_units"), F.lit(0))
).withColumn(
    "integrated_dollars", F.coalesce(F.col("integrated_dollars"), F.lit(0))
)

# ------------------------------------------------------------------------------------
# Section: Aggregation
# ------------------------------------------------------------------------------------
df_agg = df_windowed.groupBy(
    "brand_normalized_name", "normalized_name", "market_normalized_name",
    "competitor_flag", "channel_name"
).agg(
    F.sum("integrated_units").alias("total_integrated_units"),
    F.sum("integrated_normalized_units").alias("total_integrated_normalized_units"),
    F.sum("integrated_dollars").alias("total_integrated_dollars")
)

# ------------------------------------------------------------------------------------
# Section: Write Aggregated Results to Staging Table
# ------------------------------------------------------------------------------------
df_agg.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "purgo_playground.bai_sales_agg_obu_customer_datapack_channelwise_stg"
)

# ------------------------------------------------------------------------------------
# Section: Show Channel-wise Results
# ------------------------------------------------------------------------------------
# Display the result for review (can be replaced with .show() or returned as DataFrame)
df_agg.orderBy("channel_name", "brand_normalized_name").show()

# ------------------------------------------------------------------------------------
# End of Channel-wise Sales Performance Analysis
# ------------------------------------------------------------------------------------

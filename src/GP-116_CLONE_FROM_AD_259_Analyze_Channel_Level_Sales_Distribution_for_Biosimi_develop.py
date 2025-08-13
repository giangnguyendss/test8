spark.catalog.setCurrentCatalog("purgo_databricks")

# -----------------------------------------------------------------------------------
# Channel-wise Biosimilar Sales Performance Analysis
# -----------------------------------------------------------------------------------
# This script reads from purgo_playground.bai_sales_agg_obu_customer_datapack,
# filters for kanjinti, mvasi, riabni brands, channels online/retail/mobile,
# includes only records with valid cdl_effective_date (yyyy-MM-dd) and transaction_timestamp
# within 36 months from cdl_effective_date, aggregates sales metrics, and logs invalid records.
# -----------------------------------------------------------------------------------

# Required imports
from pyspark.sql import functions as F  
from pyspark.sql.types import StringType, BooleanType, TimestampType, LongType  

# -- Setup: Catalog and Schema (assume spark is already set to correct catalog/schema) --

# -- Constants --
TARGET_BRANDS = ["kanjinti", "mvasi", "riabni"]
TARGET_CHANNELS = ["online", "retail", "mobile"]
CDL_DATE_REGEX = "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"

# -- Read Source Table --
source_table = "purgo_playground.bai_sales_agg_obu_customer_datapack"
df = spark.table(source_table)

# -- Data Quality: Identify invalid cdl_effective_date records --
invalid_cdl_df = df.filter(
    (F.col("cdl_effective_date").isNull()) |
    (~F.col("cdl_effective_date").rlike(CDL_DATE_REGEX))
).withColumn(
    "error_message",
    F.when(F.col("cdl_effective_date").isNull(), F.lit("Invalid cdl_effective_date format: NULL"))
     .otherwise(F.lit("Invalid cdl_effective_date format"))
)

# -- Write invalid records to log table --
invalid_log_table = "purgo_playground.bai_sales_agg_obu_customer_datapack_invalid_cdl_effective_date_log"
# Only write if there are invalid records
if invalid_cdl_df.limit(1).count() > 0:
    invalid_cdl_df.select(
        "brand_normalized_name",
        "normalized_name",
        "market_normalized_name",
        "competitor_flag",
        "channel_name",
        "transaction_timestamp",
        "integrated_units",
        "integrated_normalized_units",
        "integrated_dollars",
        "cdl_effective_date",
        "error_message"
    ).write.mode("append").format("delta").saveAsTable(invalid_log_table)

# -- Filter for valid records only --
valid_df = df.filter(
    (F.col("cdl_effective_date").isNotNull()) &
    (F.col("cdl_effective_date").rlike(CDL_DATE_REGEX))
)

# -- Add cdl_effective_date as DateType for window calculation --
valid_df = valid_df.withColumn(
    "cdl_effective_date_dt",
    F.to_date("cdl_effective_date", "yyyy-MM-dd")
)

# -- Filter for brands, channels, and 36-month window --
filtered_df = valid_df.filter(
    F.col("brand_normalized_name").isin(TARGET_BRANDS) &
    F.col("channel_name").isin(TARGET_CHANNELS) &
    (F.col("transaction_timestamp") >= F.col("cdl_effective_date_dt")) &
    (F.col("transaction_timestamp") < F.expr("add_months(cdl_effective_date_dt, 36)")
    )
)

# -- Aggregate sales metrics by channel and key attributes --
agg_df = filtered_df.groupBy(
    "brand_normalized_name",
    "normalized_name",
    "market_normalized_name",
    "competitor_flag",
    "channel_name"
).agg(
    F.sum(F.coalesce(F.col("integrated_units"), F.lit(0))).alias("sum_integrated_units"),
    F.sum(F.coalesce(F.col("integrated_normalized_units"), F.lit(0))).alias("sum_integrated_normalized_units"),
    F.sum(F.coalesce(F.col("integrated_dollars"), F.lit(0))).alias("sum_integrated_dollars")
).orderBy(
    "brand_normalized_name", "channel_name", "normalized_name"
)

# -- Optional: Add window analytics (e.g., sales rank per brand/channel) --
from pyspark.sql.window import Window  
window_spec = Window.partitionBy("brand_normalized_name", "channel_name").orderBy(F.desc("sum_integrated_dollars"))
agg_df = agg_df.withColumn(
    "sales_rank",
    F.rank().over(window_spec)
)

# -- Show final output --
agg_df.show(truncate=False)

# -- End of script --

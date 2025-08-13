# Databricks PySpark script for data validation between product_plant and product_plant_v2
# Catalog and schema setup
# Assumes 'spark' session is available in Databricks
# Output table: purgo_playground.pp_validation_results
# Audit log table: purgo_playground.d_product_plant_processed_error_log

# /* Section: Imports */
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DecimalType
)  # built-in
from pyspark.sql import functions as F  
from pyspark.sql import Row  
from pyspark.sql.window import Window  
import time  

# /* Section: Catalog and Database Setup */
spark.catalog.setCurrentCatalog("purgo_databricks")
spark.catalog.setCurrentDatabase("purgo_playground")

# /* Section: Table Drop and Schema Definition */
spark.sql("DROP TABLE IF EXISTS purgo_playground.pp_validation_results")

pp_validation_results_schema = StructType([
    StructField("plant_id_src", IntegerType(), True),
    StructField("plant_id_tgt", IntegerType(), True),
    StructField("plant_id_validation", StringType(), True),
    StructField("plant_cd_src", StringType(), True),
    StructField("plant_cd_tgt", StringType(), True),
    StructField("plant_cd_validation", StringType(), True),
    StructField("company_cd_src", StringType(), True),
    StructField("company_cd_tgt", StringType(), True),
    StructField("company_cd_validation", StringType(), True),
    StructField("source_system_cd_src", StringType(), True),
    StructField("source_system_cd_tgt", StringType(), True),
    StructField("source_system_cd_validation", StringType(), True),
    StructField("product_line_src", StringType(), True),
    StructField("product_line_tgt", StringType(), True),
    StructField("product_line_validation", StringType(), True),
    StructField("primary_uom_cd_src", StringType(), True),
    StructField("primary_uom_cd_tgt", StringType(), True),
    StructField("primary_uom_cd_validation", StringType(), True),
    StructField("global_uom_conversion_rate_src", DecimalType(2,1), True),
    StructField("global_uom_conversion_rate_tgt", DecimalType(2,1), True),
    StructField("global_uom_conversion_rate_validation", StringType(), True),
    StructField("global_uom_cd_src", StringType(), True),
    StructField("global_uom_cd_tgt", StringType(), True),
    StructField("global_uom_cd_validation", StringType(), True),
    StructField("lifecycle_status_src", StringType(), True),
    StructField("lifecycle_status_tgt", StringType(), True),
    StructField("lifecycle_status_validation", StringType(), True),
    StructField("flais_active_src", StringType(), True),
    StructField("flais_active_tgt", StringType(), True),
    StructField("flais_active_validation", StringType(), True),
    StructField("sales_rank_abc_src", StringType(), True),
    StructField("sales_rank_abc_tgt", StringType(), True),
    StructField("sales_rank_abc_validation", StringType(), True),
    StructField("inventory_rank_abc_src", StringType(), True),
    StructField("inventory_rank_abc_tgt", StringType(), True),
    StructField("inventory_rank_abc_validation", StringType(), True),
    StructField("make_buy_src", StringType(), True),
    StructField("make_buy_tgt", StringType(), True),
    StructField("make_buy_validation", StringType(), True),
    StructField("missing_from_src", StringType(), True),
    StructField("missing_from_tgt", StringType(), True),
    StructField("error_message", StringType(), True)
])

# /* Section: Read Source Tables */
src_df = spark.table("purgo_playground.product_plant")
tgt_df = spark.table("purgo_playground.product_plant_v2")

# /* Section: Alias Columns for Source and Target */
src_df_aliased = src_df.select(
    F.col("plant_id").alias("plant_id_src"),
    F.col("plant_cd").alias("plant_cd_src"),
    F.col("company_cd").alias("company_cd_src"),
    F.col("source_system_cd").alias("source_system_cd_src"),
    F.col("product_line").alias("product_line_src"),
    F.col("primary_uom_cd").alias("primary_uom_cd_src"),
    F.col("global_uom_conversion_rate").alias("global_uom_conversion_rate_src"),
    F.col("global_uom_cd").alias("global_uom_cd_src"),
    F.col("lifecycle_status").alias("lifecycle_status_src"),
    F.col("flais_active").alias("flais_active_src"),
    F.col("sales_rank_abc").alias("sales_rank_abc_src"),
    F.col("inventory_rank_abc").alias("inventory_rank_abc_src"),
    F.col("make_buy").alias("make_buy_src")
)

tgt_df_aliased = tgt_df.select(
    F.col("plant_id").alias("plant_id_tgt"),
    F.col("plant_cd").alias("plant_cd_tgt"),
    F.col("company_cd").alias("company_cd_tgt"),
    F.col("source_system_cd").alias("source_system_cd_tgt"),
    F.col("product_line").alias("product_line_tgt"),
    F.col("primary_uom_cd").alias("primary_uom_cd_tgt"),
    F.col("global_uom_conversion_rate").alias("global_uom_conversion_rate_tgt"),
    F.col("global_uom_cd").alias("global_uom_cd_tgt"),
    F.col("lifecycle_status").alias("lifecycle_status_tgt"),
    F.col("flais_active").alias("flais_active_tgt"),
    F.col("sales_rank_abc").alias("sales_rank_abc_tgt"),
    F.col("inventory_rank_abc").alias("inventory_rank_abc_tgt"),
    F.col("make_buy").alias("make_buy_tgt")
)

# /* Section: Full Outer Join on plant_id */
joined_df = src_df_aliased.join(
    tgt_df_aliased,
    src_df_aliased["plant_id_src"] == tgt_df_aliased["plant_id_tgt"],
    how="full_outer"
)

# /* Section: Validation Logic */
def validate_col(src, tgt, col_type="string", col_name=None):
    # Returns a PySpark Column expression for validation
    if col_type == "decimal":
        # Handle decimal type comparison and conversion errors
        return F.when(
            (src.isNull() & tgt.isNull()), F.lit("Match")
        ).when(
            (src.isNull() & tgt.isNotNull()) | (src.isNotNull() & tgt.isNull()), F.lit("Mismatch")
        ).when(
            (src == tgt), F.lit("Match")
        ).otherwise(F.lit("Mismatch"))
    else:
        # String/int/null-safe comparison
        return F.when(
            (src.isNull() & tgt.isNull()), F.lit("Match")
        ).when(
            (src.isNull() & tgt.isNotNull()) | (src.isNotNull() & tgt.isNull()), F.lit("Mismatch")
        ).when(
            (src == tgt), F.lit("Match")
        ).otherwise(F.lit("Mismatch"))

def validate_decimal_with_error(src, tgt, src_raw, tgt_raw, col_name):
    # Returns validation column and error_message column for decimal fields
    # src_raw/tgt_raw are the original columns before casting
    return (
        F.when(
            (src.isNull() & tgt.isNull()), F.lit("Match")
        ).when(
            (src.isNull() & tgt.isNotNull()) | (src.isNotNull() & tgt.isNull()), F.lit("Mismatch")
        ).when(
            (src == tgt), F.lit("Match")
        ).otherwise(F.lit("Mismatch")),
        F.when(
            (src_raw.isNotNull() & ~F.col(col_name + "_src").cast("decimal(2,1)").isNotNull()), F.lit(f"Data type mismatch for {col_name}")
        ).when(
            (tgt_raw.isNotNull() & ~F.col(col_name + "_tgt").cast("decimal(2,1)").isNotNull()), F.lit(f"Data type mismatch for {col_name}")
        ).otherwise(F.lit(None))
    )

# /* Section: Build Validation Columns */
validation_df = joined_df.withColumn(
    "plant_id_validation", validate_col(F.col("plant_id_src"), F.col("plant_id_tgt"), "int", "plant_id")
).withColumn(
    "plant_cd_validation", validate_col(F.col("plant_cd_src"), F.col("plant_cd_tgt"), "string", "plant_cd")
).withColumn(
    "company_cd_validation", validate_col(F.col("company_cd_src"), F.col("company_cd_tgt"), "string", "company_cd")
).withColumn(
    "source_system_cd_validation", validate_col(F.col("source_system_cd_src"), F.col("source_system_cd_tgt"), "string", "source_system_cd")
).withColumn(
    "product_line_validation", validate_col(F.col("product_line_src"), F.col("product_line_tgt"), "string", "product_line")
).withColumn(
    "primary_uom_cd_validation", validate_col(F.col("primary_uom_cd_src"), F.col("primary_uom_cd_tgt"), "string", "primary_uom_cd")
).withColumn(
    "global_uom_conversion_rate_src", F.col("global_uom_conversion_rate_src").cast("decimal(2,1)")
).withColumn(
    "global_uom_conversion_rate_tgt", F.col("global_uom_conversion_rate_tgt").cast("decimal(2,1)")
).withColumn(
    "global_uom_conversion_rate_validation", validate_col(F.col("global_uom_conversion_rate_src"), F.col("global_uom_conversion_rate_tgt"), "decimal", "global_uom_conversion_rate")
).withColumn(
    "global_uom_cd_validation", validate_col(F.col("global_uom_cd_src"), F.col("global_uom_cd_tgt"), "string", "global_uom_cd")
).withColumn(
    "lifecycle_status_validation", validate_col(F.col("lifecycle_status_src"), F.col("lifecycle_status_tgt"), "string", "lifecycle_status")
).withColumn(
    "flais_active_validation", validate_col(F.col("flais_active_src"), F.col("flais_active_tgt"), "string", "flais_active")
).withColumn(
    "sales_rank_abc_validation", validate_col(F.col("sales_rank_abc_src"), F.col("sales_rank_abc_tgt"), "string", "sales_rank_abc")
).withColumn(
    "inventory_rank_abc_validation", validate_col(F.col("inventory_rank_abc_src"), F.col("inventory_rank_abc_tgt"), "string", "inventory_rank_abc")
).withColumn(
    "make_buy_validation", validate_col(F.col("make_buy_src"), F.col("make_buy_tgt"), "string", "make_buy")
)

# /* Section: Missing Row Handling */
validation_df = validation_df.withColumn(
    "missing_from_src",
    F.when(F.col("plant_id_src").isNull() & F.col("plant_id_tgt").isNotNull(), F.lit("Missing in Source"))
).withColumn(
    "missing_from_tgt",
    F.when(F.col("plant_id_tgt").isNull() & F.col("plant_id_src").isNotNull(), F.lit("Missing in Target"))
)

# /* Section: Error Handling for Decimal Conversion */
validation_df = validation_df.withColumn(
    "error_message",
    F.when(
        (F.col("global_uom_conversion_rate_src").isNull() & F.col("global_uom_conversion_rate_tgt").isNotNull() & F.col("global_uom_conversion_rate_tgt").cast("decimal(2,1)").isNull()),
        F.lit("Data type mismatch for global_uom_conversion_rate")
    ).when(
        (F.col("global_uom_conversion_rate_tgt").isNull() & F.col("global_uom_conversion_rate_src").isNotNull() & F.col("global_uom_conversion_rate_src").cast("decimal(2,1)").isNull()),
        F.lit("Data type mismatch for global_uom_conversion_rate")
    ).otherwise(F.lit(None))
)

# /* Section: Unexpected Exception Handling */
def set_error_row(row):
    # If any error_message is set, set all validation columns to "Error"
    if row.error_message is not None and "exception" in row.error_message.lower():
        for col in [
            "plant_id_validation", "plant_cd_validation", "company_cd_validation", "source_system_cd_validation",
            "product_line_validation", "primary_uom_cd_validation", "global_uom_conversion_rate_validation",
            "global_uom_cd_validation", "lifecycle_status_validation", "flais_active_validation",
            "sales_rank_abc_validation", "inventory_rank_abc_validation", "make_buy_validation"
        ]:
            setattr(row, col, "Error")
    return row

validation_df = validation_df.rdd.map(set_error_row).toDF(validation_df.schema)

# /* Section: Column Order and Selection */
final_cols = [
    "plant_id_src", "plant_id_tgt", "plant_id_validation",
    "plant_cd_src", "plant_cd_tgt", "plant_cd_validation",
    "company_cd_src", "company_cd_tgt", "company_cd_validation",
    "source_system_cd_src", "source_system_cd_tgt", "source_system_cd_validation",
    "product_line_src", "product_line_tgt", "product_line_validation",
    "primary_uom_cd_src", "primary_uom_cd_tgt", "primary_uom_cd_validation",
    "global_uom_conversion_rate_src", "global_uom_conversion_rate_tgt", "global_uom_conversion_rate_validation",
    "global_uom_cd_src", "global_uom_cd_tgt", "global_uom_cd_validation",
    "lifecycle_status_src", "lifecycle_status_tgt", "lifecycle_status_validation",
    "flais_active_src", "flais_active_tgt", "flais_active_validation",
    "sales_rank_abc_src", "sales_rank_abc_tgt", "sales_rank_abc_validation",
    "inventory_rank_abc_src", "inventory_rank_abc_tgt", "inventory_rank_abc_validation",
    "make_buy_src", "make_buy_tgt", "make_buy_validation",
    "missing_from_src", "missing_from_tgt", "error_message"
]

validation_df = validation_df.select(final_cols)

# /* Section: Write to Delta Table */
validation_df.write.mode("overwrite").format("delta").saveAsTable("purgo_playground.pp_validation_results")

# /* Section: Audit Trail and Logging */
stats = validation_df.groupBy().agg(
    F.count("*").alias("total_rows"),
    F.sum(F.when(F.col("plant_id_validation") == "Match", 1).otherwise(0)).alias("match_count"),
    F.sum(F.when(F.col("plant_id_validation") == "Mismatch", 1).otherwise(0)).alias("mismatch_count"),
    F.sum(F.when(F.col("plant_id_validation") == "Error", 1).otherwise(0)).alias("error_count")
).collect()[0]

log_row = Row(
    error_message=f"Validation summary: {stats['total_rows']} rows, {stats['match_count']} match, {stats['mismatch_count']} mismatch, {stats['error_count']} error",
    timestamp=str(int(time.time())),
    row_details=None
)
log_df = spark.createDataFrame([log_row])
log_df.write.mode("append").format("delta").saveAsTable("purgo_playground.d_product_plant_processed_error_log")

# /* Section: Validation Statistics Display Using CTE */
validation_stats_cte = """
WITH validation_stats AS (
    SELECT
        COUNT(*) AS total_rows,
        SUM(CASE WHEN plant_id_validation = 'Match' THEN 1 ELSE 0 END) AS plant_id_match_count,
        SUM(CASE WHEN plant_id_validation = 'Mismatch' THEN 1 ELSE 0 END) AS plant_id_mismatch_count,
        SUM(CASE WHEN plant_id_validation = 'Error' THEN 1 ELSE 0 END) AS plant_id_error_count,
        SUM(CASE WHEN error_message IS NOT NULL THEN 1 ELSE 0 END) AS error_row_count
    FROM purgo_playground.pp_validation_results
)
SELECT * FROM validation_stats
"""
display(spark.sql(validation_stats_cte))
# End of script

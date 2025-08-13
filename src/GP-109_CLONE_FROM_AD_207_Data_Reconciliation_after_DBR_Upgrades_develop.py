spark.catalog.setCurrentCatalog("purgo_databricks")

# ----------------------------------------------------------------------------------------
# Databricks PySpark Script: Product Plant Table Comparison and Validation
# Catalog: purgo_databricks
# Schema: purgo_playground
# Output Table: pp_validation_results (overwritten each run)
# ----------------------------------------------------------------------------------------
# This script compares purgo_playground.product_plant and purgo_playground.product_plant_v2
# and generates a validation table with column-wise match/mismatch indicators.
# - All columns are aliased as _src (product_plant) and _tgt (product_plant_v2)
# - Output columns are ordered: src, tgt, validation for each field
# - Handles missing rows, duplicate keys, nulls, decimal precision, and error logging
# ----------------------------------------------------------------------------------------

# Commented out SparkSession initialization (spark is already available in Databricks)
# from pyspark.sql import SparkSession  # built-in
# spark = SparkSession.builder.getOrCreate()

from pyspark.sql.functions import col, when, lit, round, count  

# ----------------------------------------------------------------------------------------
# SECTION: Duplicate Key Detection
# ----------------------------------------------------------------------------------------
def get_duplicate_keys(df, key_col):
    return df.groupBy(key_col).count().filter(col("count") > 1).select(key_col)

df_product_plant = spark.table("purgo_playground.product_plant")
df_product_plant_v2 = spark.table("purgo_playground.product_plant_v2")

duplicate_src_keys = [row["plant_id"] for row in get_duplicate_keys(df_product_plant, "plant_id").collect()]
duplicate_tgt_keys = [row["plant_id"] for row in get_duplicate_keys(df_product_plant_v2, "plant_id").collect()]

# ----------------------------------------------------------------------------------------
# SECTION: Data Comparison and Validation Logic
# ----------------------------------------------------------------------------------------
# Outer join to handle missing rows in either table
df_joined = df_product_plant.alias("src").join(
    df_product_plant_v2.alias("tgt"),
    on=col("src.plant_id") == col("tgt.plant_id"),
    how="outer"
)

def validate_col(src_col, tgt_col, col_type="string"):
    if col_type == "decimal":
        # Compare up to one decimal place, nulls match only if both null
        return when(
            (col(src_col).isNull() & col(tgt_col).isNull()), lit("Match")
        ).when(
            (col(src_col).isNotNull() & col(tgt_col).isNotNull()) &
            (round(col(src_col),1) == round(col(tgt_col),1)), lit("Match")
        ).otherwise(lit("Mismatch"))
    else:
        # String columns: nulls match only if both null, else compare
        return when(
            (col(src_col).isNull() & col(tgt_col).isNull()), lit("Match")
        ).when(
            (col(src_col) == col(tgt_col)), lit("Match")
        ).otherwise(lit("Mismatch"))

validation_exprs = [
    col("src.plant_id").alias("plant_id_src"),
    col("tgt.plant_id").alias("plant_id_tgt"),
    validate_col("src.plant_id", "tgt.plant_id", "string").alias("plant_id_validation"),
    col("src.plant_cd").alias("plant_cd_src"),
    col("tgt.plant_cd").alias("plant_cd_tgt"),
    validate_col("src.plant_cd", "tgt.plant_cd", "string").alias("plant_cd_validation"),
    col("src.company_cd").alias("company_cd_src"),
    col("tgt.company_cd").alias("company_cd_tgt"),
    validate_col("src.company_cd", "tgt.company_cd", "string").alias("company_cd_validation"),
    col("src.source_system_cd").alias("source_system_cd_src"),
    col("tgt.source_system_cd").alias("source_system_cd_tgt"),
    validate_col("src.source_system_cd", "tgt.source_system_cd", "string").alias("source_system_cd_validation"),
    col("src.product_line").alias("product_line_src"),
    col("tgt.product_line").alias("product_line_tgt"),
    validate_col("src.product_line", "tgt.product_line", "string").alias("product_line_validation"),
    col("src.primary_uom_cd").alias("primary_uom_cd_src"),
    col("tgt.primary_uom_cd").alias("primary_uom_cd_tgt"),
    validate_col("src.primary_uom_cd", "tgt.primary_uom_cd", "string").alias("primary_uom_cd_validation"),
    col("src.global_uom_conversion_rate").alias("global_uom_conversion_rate_src"),
    col("tgt.global_uom_conversion_rate").alias("global_uom_conversion_rate_tgt"),
    validate_col("src.global_uom_conversion_rate", "tgt.global_uom_conversion_rate", "decimal").alias("global_uom_conversion_rate_validation"),
    col("src.global_uom_cd").alias("global_uom_cd_src"),
    col("tgt.global_uom_cd").alias("global_uom_cd_tgt"),
    validate_col("src.global_uom_cd", "tgt.global_uom_cd", "string").alias("global_uom_cd_validation"),
    col("src.lifecycle_status").alias("lifecycle_status_src"),
    col("tgt.lifecycle_status").alias("lifecycle_status_tgt"),
    validate_col("src.lifecycle_status", "tgt.lifecycle_status", "string").alias("lifecycle_status_validation"),
    col("src.flais_active").alias("flais_active_src"),
    col("tgt.flais_active").alias("flais_active_tgt"),
    validate_col("src.flais_active", "tgt.flais_active", "string").alias("flais_active_validation"),
    col("src.sales_rank_abc").alias("sales_rank_abc_src"),
    col("tgt.sales_rank_abc").alias("sales_rank_abc_tgt"),
    validate_col("src.sales_rank_abc", "tgt.sales_rank_abc", "string").alias("sales_rank_abc_validation"),
    col("src.inventory_rank_abc").alias("inventory_rank_abc_src"),
    col("tgt.inventory_rank_abc").alias("inventory_rank_abc_tgt"),
    validate_col("src.inventory_rank_abc", "tgt.inventory_rank_abc", "string").alias("inventory_rank_abc_validation"),
    col("src.make_buy").alias("make_buy_src"),
    col("tgt.make_buy").alias("make_buy_tgt"),
    validate_col("src.make_buy", "tgt.make_buy", "string").alias("make_buy_validation"),
    when(col("src.plant_id").isNull(), lit("Y")).otherwise(lit("N")).alias("missing_from_src"),
    when(col("tgt.plant_id").isNull(), lit("Y")).otherwise(lit("N")).alias("missing_from_tgt"),
    when(col("src.plant_id").isin(*duplicate_src_keys), lit("Duplicate plant_id found in product_plant"))
    .when(col("tgt.plant_id").isin(*duplicate_tgt_keys), lit("Duplicate plant_id found in product_plant_v2"))
    .otherwise(lit("")).alias("error_message")
]

df_validation = df_joined.select(*validation_exprs)

# ----------------------------------------------------------------------------------------
# SECTION: Table Creation and Overwrite
# ----------------------------------------------------------------------------------------
spark.sql("DROP TABLE IF EXISTS purgo_playground.pp_validation_results")
df_validation.write.mode("overwrite").saveAsTable("purgo_playground.pp_validation_results")

# Commented out spark.stop() as it can cause issues in Databricks
# spark.stop()

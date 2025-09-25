# Batch Quality Analysis and Production Summary for purgo_playground.batch_qc
# Unity Catalog: purgo_databricks
# Schema: purgo_playground
# Table: batch_qc
# Output: product-wise analysis, batch-wise quality status, batch-wise production summary
# All code follows Databricks PySpark, Unity Catalog, and performance best practices

# from pyspark.sql import SparkSession  # SparkSession is already available in Databricks

# Import required PySpark modules
from pyspark.sql.types import StringType, DoubleType  
from pyspark.sql.functions import col, when, count, sum as spark_sum, round  
from pyspark.sql import Row  

# Set current catalog for Unity Catalog operations
spark.catalog.setCurrentCatalog("purgo_databricks")  # Ensure correct catalog context

# -------------------- Read Source Table --------------------
# Read the batch_qc table from Unity Catalog
batch_qc_df = spark.table("purgo_playground.batch_qc")

# -------------------- Schema Validation --------------------
# Validate required columns and types
required_columns = {
    "batch_id": StringType(),
    "product_name": StringType(),
    "quality_check_score": DoubleType()
}

for col_name, col_type in required_columns.items():
    if col_name not in batch_qc_df.columns:
        raise Exception(f"Missing required column: {col_name}")
    actual_type = [f.dataType for f in batch_qc_df.schema.fields if f.name == col_name][0]
    if type(actual_type) != type(col_type):
        raise Exception(f"Type mismatch for column: {col_name}")

# Check for nulls in required columns
for col_name in required_columns.keys():
    null_count = batch_qc_df.filter(col(col_name).isNull()).count()
    if null_count > 0:
        raise Exception(f"Null value in required column: {col_name}")

# Check for duplicate batch_id and log warning
dup_batch_ids = batch_qc_df.groupBy("batch_id").agg(count("*").alias("cnt")).filter(col("cnt") > 1)
if dup_batch_ids.count() > 0:
    for row in dup_batch_ids.collect():
        print(f"Warning: Duplicate batch_id detected: {row['batch_id']}")

# -------------------- Data Type Conversion and NULL Handling --------------------
# Ensure all quality_check_score values are numeric and not null
non_numeric_scores = batch_qc_df.filter(~col("quality_check_score").cast(DoubleType()).isNotNull())
if non_numeric_scores.count() > 0:
    for row in non_numeric_scores.collect():
        raise Exception(f"Invalid type for quality_check_score in batch {row['batch_id']}")

# -------------------- Transformation: Add Pass/Fail Status --------------------
# Add status column: pass/fail based on quality_check_score
batch_qc_df = batch_qc_df.withColumn(
    "status",
    when(col("quality_check_score") < 97, "fail").otherwise("pass")
)

# -------------------- Product-wise Batch Analysis --------------------
# CTE: product_analysis_cte
product_analysis_cte = (
    batch_qc_df
    .groupBy("product_name")
    .agg(
        count("*").alias("total_batches"),
        spark_sum(when(col("status") == "pass", 1).otherwise(0)).alias("passed_batches"),
        spark_sum(when(col("status") == "fail", 1).otherwise(0)).alias("failed_batches"),
        round(
            (spark_sum(when(col("status") == "pass", 1).otherwise(0)) / count("*")) * 100, 2
        ).alias("percent_passed")
    )
)
# Columns: product_name, total_batches, passed_batches, failed_batches, percent_passed

# -------------------- Batch-wise Quality Status --------------------
# CTE: batch_quality_status_cte
batch_quality_status_cte = batch_qc_df.select(
    "batch_id", "product_name", "quality_check_score", "status"
)
# Columns: batch_id, product_name, quality_check_score, status

# -------------------- Batch-wise Production Summary --------------------
# CTE: batch_summary_cte
total_batches = batch_qc_df.count()
total_passed_batches = batch_qc_df.filter(col("status") == "pass").count()
total_failed_batches = batch_qc_df.filter(col("status") == "fail").count()

batch_summary_cte = spark.createDataFrame([
    Row(
        total_batches=total_batches,
        total_passed_batches=total_passed_batches,
        total_failed_batches=total_failed_batches
    )
])
# Columns: total_batches, total_passed_batches, total_failed_batches

# -------------------- Final Output DataFrames --------------------
# product_analysis_cte: Product-wise analysis
# batch_quality_status_cte: Batch-wise quality status
# batch_summary_cte: Batch-wise production summary

# Uncomment below to display results in Databricks notebook
# display(product_analysis_cte)
# display(batch_quality_status_cte)
# display(batch_summary_cte)

# spark.stop()  # Do not stop SparkSession in Databricks

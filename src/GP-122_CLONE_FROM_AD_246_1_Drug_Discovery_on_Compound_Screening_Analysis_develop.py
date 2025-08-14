spark.catalog.setCurrentCatalog("purgo_databricks")

# /* 
# Compound Drug Analysis Aggregation and Potential Categorization
# PySpark implementation for purgo_playground.compound_drug_analysis
# Performs: Filtering, Aggregation, Join, Categorization, Data Quality, Schema, Data Types, Edge Cases
# Assumes 'spark' session is available in Databricks
# */

# from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql import functions as F  
from pyspark.sql.types import (  
    StructType, StructField, StringType, DoubleType, LongType
)
from pyspark.sql.window import Window  

# /* Section: Setup and Schema Validation */

# Define expected schema for purgo_playground.compound_drug_analysis
expected_schema = StructType([
    StructField("study_id", StringType(), True),
    StructField("compound_id", StringType(), True),
    StructField("mutation_id", StringType(), True),
    StructField("therapeutic_area", StringType(), True),
    StructField("drug_name", StringType(), True),
    StructField("ic50", DoubleType(), True),
    StructField("auc", DoubleType(), True),
    StructField("efficacy", DoubleType(), True),
    StructField("toxicity", DoubleType(), True),
    StructField("potency", DoubleType(), True),
    StructField("sample_size", LongType(), True),
    StructField("mutation_frequency", LongType(), True),
    StructField("mutation_severity", LongType(), True),
    StructField("compound_concentration", DoubleType(), True),
    StructField("cell_viability", DoubleType(), True),
    StructField("growth_inhibition", DoubleType(), True),
    StructField("result", StringType(), True),
    StructField("approved_flag", LongType(), True),
    StructField("validation_status", StringType(), True),
    StructField("status", StringType(), True),
    StructField("created_by", StringType(), True),
    StructField("score1", DoubleType(), True),
    StructField("score2", DoubleType(), True),
    StructField("score3", DoubleType(), True),
    StructField("score4", DoubleType(), True),
    StructField("score5", DoubleType(), True)
])

# Read the table and validate schema
df = spark.read.table("purgo_playground.compound_drug_analysis")
assert df.schema == expected_schema, "Schema mismatch in compound_drug_analysis"

# /* Section: Data Quality Validation - Exclude rows with NULLs in required columns */
required_cols = [
    "ic50", "auc", "efficacy", "score1", "score2", "score3", "score4", "score5", "therapeutic_area"
]
from functools import reduce
not_null_expr = reduce(
    lambda a, b: a & b,
    [F.col(c).isNotNull() for c in required_cols]
)
df_not_null = df.filter(not_null_expr)

# /* Section: Data Type Validation for Filtering */
# Ensure approved_flag is integer 1 and validation_status is string "valid"
df_filtered = df_not_null.filter(
    (F.col("approved_flag") == 1) & (F.col("validation_status") == "valid")
)

# /* Section: Exclude duplicate or missing therapeutic_area values */
# Remove rows with NULL therapeutic_area (already handled above)
# Remove duplicate rows based on all columns (for grouping, only unique therapeutic_area needed)
df_filtered = df_filtered.dropDuplicates()

# /* Section: Aggregation by therapeutic_area */
agg_df = df_filtered.groupBy("therapeutic_area").agg(
    F.avg("ic50").alias("average_ic50"),
    F.avg("auc").alias("average_auc"),
    F.avg("efficacy").alias("average_efficacy"),
    F.sum("sample_size").alias("total_sample_size"),
    F.countDistinct("study_id").alias("study_count")
)

# /* Section: Join Analysis - Inner Join on therapeutic_area */
joined_df = df_filtered.join(
    agg_df,
    on="therapeutic_area",
    how="inner"
)

# /* Section: Result Analysis - overall_score calculation and categorization */
# Calculate overall_score as average of non-null score1..score5 per row
score_cols = ["score1", "score2", "score3", "score4", "score5"]
joined_df = joined_df.withColumn(
    "overall_score",
    F.expr("ROUND((score1 + score2 + score3 + score4 + score5) / 5, 2)")
)

# Categorize potential_category based on overall_score
joined_df = joined_df.withColumn(
    "potential_category",
    F.when((F.col("overall_score") >= 70) & (F.col("overall_score") <= 100), "High Potential")
     .when((F.col("overall_score") >= 60) & (F.col("overall_score") < 70), "Moderate Potential")
     .when(F.col("overall_score") < 60, "Low Potential")
     .otherwise(F.lit(None))
)

# /* Section: Final Output - Select all columns plus aggregates and result analysis */
final_cols = df.columns + [
    "average_ic50", "average_auc", "average_efficacy", "total_sample_size", "study_count", "overall_score", "potential_category"
]
final_df = joined_df.select(*final_cols)

# /* Section: Window Function - Analytics Feature */
w = Window.partitionBy("therapeutic_area").orderBy(F.desc("overall_score"))
final_df = final_df.withColumn("rank_in_area", F.row_number().over(w))

# /* Section: Display Final Output */
final_df.show(truncate=False)

# /* End of PySpark implementation for Compound Drug Analysis */

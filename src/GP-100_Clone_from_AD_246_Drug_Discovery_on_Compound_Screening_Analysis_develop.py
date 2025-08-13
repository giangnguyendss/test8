spark.catalog.setCurrentCatalog("purgo_databricks")

# /* 
# Compound Drug Analysis Aggregation and Potential Categorization
# PySpark implementation for purgo_playground.compound_drug_analysis
# Unity Catalog: purgo_databricks
# Schema: purgo_playground
#
# This script:
#   - Reads compound_drug_analysis from Unity Catalog
#   - Filters for approved_flag = 1 and validation_status = "valid"
#   - Aggregates by therapeutic_area: avg_ic50, avg_auc, avg_efficacy, total_sample_size, study_count
#   - Joins filtered data with aggregated metrics on therapeutic_area
#   - Calculates overall_score as mean of non-null score1..score5
#   - Categorizes each row as High/Moderate/Low Potential based on overall_score
#   - Handles nulls, out-of-range, and invalid types
#   - Displays all columns from compound_drug_analysis plus analysis columns
#   - Follows Databricks best practices and PySpark patterns
# */

# ----------------------------------------
# Imports
# ----------------------------------------
from pyspark.sql import functions as F  
from pyspark.sql.types import DoubleType, LongType, StringType  
from pyspark.sql.utils import AnalysisException  

# ----------------------------------------
# Read source table from Unity Catalog
# ----------------------------------------
try:
    df = spark.read.table("purgo_playground.compound_drug_analysis")
except AnalysisException as e:
    # Handle missing table gracefully
    raise RuntimeError(f"Table not found: {e}")

# ----------------------------------------
# Data Quality: Validate required columns and types
# ----------------------------------------
required_cols = [
    "study_id", "compound_id", "mutation_id", "therapeutic_area", "drug_name",
    "ic50", "auc", "efficacy", "sample_size",
    "approved_flag", "validation_status",
    "score1", "score2", "score3", "score4", "score5"
]
missing_cols = [c for c in required_cols if c not in df.columns]
if missing_cols:
    raise RuntimeError(f"Missing required column(s): {', '.join(missing_cols)}")

# Convert critical columns to correct types, handle invalids as nulls
for col, dtype in [("ic50", DoubleType()), ("auc", DoubleType()), ("efficacy", DoubleType()), ("sample_size", LongType()),
                   ("score1", DoubleType()), ("score2", DoubleType()), ("score3", DoubleType()), ("score4", DoubleType()), ("score5", DoubleType())]:
    df = df.withColumn(col, F.col(col).cast(dtype))

# ----------------------------------------
# Filter: Only approved and valid rows
# ----------------------------------------
filtered_df = df.filter(
    (F.col("approved_flag") == 1) & (F.col("validation_status") == "valid")
)

# ----------------------------------------
# Aggregation: By therapeutic_area
# ----------------------------------------
agg_df = filtered_df.groupBy("therapeutic_area").agg(
    F.avg("ic50").alias("avg_ic50"),
    F.avg("auc").alias("avg_auc"),
    F.avg("efficacy").alias("avg_efficacy"),
    F.sum("sample_size").alias("total_sample_size"),
    F.count("study_id").alias("study_count")
)

# ----------------------------------------
# Join: Filtered data with aggregated metrics
# ----------------------------------------
joined_df = filtered_df.join(
    agg_df,
    on="therapeutic_area",
    how="inner"
)

# ----------------------------------------
# Result Analysis: overall_score calculation
# ----------------------------------------
# Calculate overall_score as mean of non-null score1..score5
joined_df = joined_df.withColumn(
    "score_array",
    F.array("score1", "score2", "score3", "score4", "score5")
).withColumn(
    "non_null_scores",
    F.expr("filter(score_array, x -> x is not null)")
).withColumn(
    "overall_score",
    F.when(
        F.size(F.col("non_null_scores")) == 0,
        F.lit(None).cast(DoubleType())
    ).otherwise(
        F.expr("aggregate(non_null_scores, 0D, (acc, x) -> acc + x, acc -> acc / size(non_null_scores))")
    )
)

# ----------------------------------------
# Data Quality: Out-of-range score values (0-100)
# ----------------------------------------
out_of_range_cond = (
    (F.col("score1") > 100) | (F.col("score1") < 0) |
    (F.col("score2") > 100) | (F.col("score2") < 0) |
    (F.col("score3") > 100) | (F.col("score3") < 0) |
    (F.col("score4") > 100) | (F.col("score4") < 0) |
    (F.col("score5") > 100) | (F.col("score5") < 0)
)
joined_df = joined_df.withColumn(
    "score_out_of_range",
    F.when(out_of_range_cond, F.lit(True)).otherwise(F.lit(False))
)

# ----------------------------------------
# Categorization: High/Moderate/Low Potential
# ----------------------------------------
joined_df = joined_df.withColumn(
    "potential_category",
    F.when(
        (F.col("score_out_of_range") == True) | (F.col("overall_score").isNull()),
        F.lit(None).cast(StringType())
    ).when(
        (F.col("overall_score") >= 70) & (F.col("overall_score") <= 100),
        F.lit("High Potential")
    ).when(
        (F.col("overall_score") >= 60) & (F.col("overall_score") < 70),
        F.lit("Moderate Potential")
    ).when(
        (F.col("overall_score") < 60),
        F.lit("Low Potential")
    ).otherwise(F.lit(None).cast(StringType()))
)

# ----------------------------------------
# Final Output: Select all columns + analysis columns
# ----------------------------------------
final_cols = list(df.columns) + [
    "avg_ic50", "avg_auc", "avg_efficacy", "total_sample_size", "study_count", "overall_score", "potential_category"
]
final_output = joined_df.select(*final_cols)

# ----------------------------------------
# Display results
# ----------------------------------------
final_output.show(truncate=False)

# /* End of PySpark implementation for Compound Drug Analysis */

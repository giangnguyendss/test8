# /*
# Compound Drug Screening Analysis for Databricks PySpark
# - Reads purgo_playground.compound_drug_analysis
# - Filters for approved_flag == 1 and validation_status == "valid"
# - Aggregates by therapeutic_area: avg_ic50, avg_auc, avg_efficacy, total_sample_size, study_count
# - Joins filtered data with aggregated metrics on therapeutic_area
# - Computes overall_score (average of score1-5, ignoring nulls)
# - Categorizes potential_category: High/Moderate/Low Potential
# - Displays all source columns + aggregated metrics + overall_score + potential_category
# - Handles nulls, schema validation, and data quality checks
# - All code is Databricks-compatible and follows best practices
# */

# from pyspark.sql import SparkSession  # SparkSession is already available in Databricks

# -- Required imports for PySpark DataFrame operations
from pyspark.sql.functions import col, avg, sum, countDistinct, when, lit, isnan, isnull, udf  
from pyspark.sql.types import DoubleType  

# /* Section: Setup and Configuration */
spark.catalog.setCurrentCatalog("purgo_databricks")
spark.catalog.setCurrentDatabase("purgo_playground")

# /* Section: Schema Validation */
expected_columns = [
    "study_id", "compound_id", "mutation_id", "therapeutic_area", "drug_name", "ic50", "auc", "efficacy", "toxicity", "potency",
    "sample_size", "mutation_frequency", "mutation_severity", "compound_concentration", "cell_viability", "growth_inhibition",
    "result", "approved_flag", "validation_status", "status", "created_by", "score1", "score2", "score3", "score4", "score5"
]
source_df = spark.table("purgo_playground.compound_drug_analysis")
actual_columns = source_df.columns
if len(actual_columns) != len(expected_columns):
    raise Exception(f"Column count mismatch: expected {len(expected_columns)}, got {len(actual_columns)}")
for col_name in expected_columns:
    if col_name not in actual_columns:
        raise Exception(f"Missing column: {col_name}")

# /* Section: Filter Analysis */
filtered_df = source_df.filter(
    (col("approved_flag") == 1) & (col("validation_status") == "valid")
)

# /* Section: Aggregation Analysis */
agg_df = filtered_df.groupBy("therapeutic_area").agg(
    avg(col("ic50")).alias("avg_ic50"),
    avg(col("auc")).alias("avg_auc"),
    avg(col("efficacy")).alias("avg_efficacy"),
    sum(when(isnull(col("sample_size")), lit(0)).otherwise(col("sample_size"))).alias("total_sample_size"),
    countDistinct(col("study_id")).alias("study_count")
)

# /* Section: Join Analysis */
joined_df = filtered_df.join(
    agg_df,
    on="therapeutic_area",
    how="left"
)

# /* Section: Result Analysis */
def calc_overall_score(score1, score2, score3, score4, score5):
    scores = [score1, score2, score3, score4, score5]
    valid_scores = [s for s in scores if s is not None]
    if not valid_scores:
        return None
    return float(sum(valid_scores)) / len(valid_scores)

overall_score_udf = udf(calc_overall_score, DoubleType())

result_df = joined_df.withColumn(
    "overall_score",
    overall_score_udf(col("score1"), col("score2"), col("score3"), col("score4"), col("score5"))
).withColumn(
    "potential_category",
    when((col("overall_score") >= 70) & (col("overall_score") <= 100), lit("High Potential"))
    .when((col("overall_score") >= 60) & (col("overall_score") < 70), lit("Moderate Potential"))
    .otherwise(lit("Low Potential"))
)

# /* Section: Data Quality Checks */
# -- Check for NULLs in key metrics
null_ic50_count = result_df.filter(isnull(col("ic50"))).count()
null_auc_count = result_df.filter(isnull(col("auc"))).count()
null_efficacy_count = result_df.filter(isnull(col("efficacy"))).count()
null_sample_size_count = result_df.filter(isnull(col("sample_size"))).count()
# -- Check for NaN in numeric columns
for metric in ["ic50", "auc", "efficacy", "sample_size", "score1", "score2", "score3", "score4", "score5"]:
    nan_count = result_df.filter(isnan(col(metric))).count()
    if nan_count > 0:
        raise Exception(f"Invalid value (NaN) found in column {metric}")

# /* Section: Final Output Display */
final_columns = [
    "study_id", "compound_id", "mutation_id", "therapeutic_area", "drug_name", "ic50", "auc", "efficacy", "toxicity", "potency",
    "sample_size", "mutation_frequency", "mutation_severity", "compound_concentration", "cell_viability", "growth_inhibition",
    "result", "approved_flag", "validation_status", "status", "created_by", "score1", "score2", "score3", "score4", "score5",
    "avg_ic50", "avg_auc", "avg_efficacy", "total_sample_size", "study_count", "overall_score", "potential_category"
]
result_df.select(final_columns).show(truncate=False)

# /* End of Compound Drug Screening Analysis */
# spark.stop()  # Do not stop SparkSession in Databricks

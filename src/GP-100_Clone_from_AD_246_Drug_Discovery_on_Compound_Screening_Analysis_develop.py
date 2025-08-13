spark.catalog.setCurrentCatalog("purgo_databricks")

# ------------------------------------------------------------------------------------
# Compound Drug Analysis Aggregation and Categorization - PySpark Implementation
# ------------------------------------------------------------------------------------
# This script performs the following:
# - Reads compound_drug_analysis from Unity Catalog
# - Filters for approved_flag = 1 and validation_status = 'valid'
# - Groups by therapeutic_area to calculate avg_ic50, avg_auc, avg_efficacy, total_sample_size, study_count
# - Joins filtered data with aggregated data on therapeutic_area
# - Calculates overall_score (average of non-null score1..score5)
# - Assigns potential_category based on overall_score
# - Outputs all columns from compound_drug_analysis plus aggregation and result columns
# ------------------------------------------------------------------------------------
# Assumptions:
# - spark session is available in Databricks
# - Table purgo_playground.compound_drug_analysis exists and is accessible
# - No temp views or temp tables are used
# - All code is Databricks-compatible
# ------------------------------------------------------------------------------------

# from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql import functions as F  
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType  

# ------------------------------------------------------------------------------------
# Section: Helper UDFs
# ------------------------------------------------------------------------------------

# UDF to calculate overall_score (average of non-null score1..score5, rounded to 2 decimals)
overall_score_udf = F.udf(
    lambda s1, s2, s3, s4, s5: (
        round(
            sum([x for x in [s1, s2, s3, s4, s5] if x is not None]) /
            len([x for x in [s1, s2, s3, s4, s5] if x is not None]), 2
        ) if len([x for x in [s1, s2, s3, s4, s5] if x is not None]) > 0 else None
    ),
    DoubleType()
)

# UDF to assign potential_category based on overall_score
def assign_potential_category(score):
    if score is None:
        return None
    if 70 <= score <= 100:
        return "High Potential"
    elif 60 <= score < 70:
        return "Moderate Potential"
    elif score < 60:
        return "Low Potential"
    else:
        return None

potential_category_udf = F.udf(assign_potential_category, StringType())

# ------------------------------------------------------------------------------------
# Section: Read Source Table
# ------------------------------------------------------------------------------------

df = spark.table("purgo_playground.compound_drug_analysis")

# ------------------------------------------------------------------------------------
# Section: Filter Analysis
# ------------------------------------------------------------------------------------

filtered_df = df.filter(
    (F.col("approved_flag") == 1) &
    (F.col("validation_status") == "valid") &
    (F.col("therapeutic_area").isNotNull())
)

# ------------------------------------------------------------------------------------
# Section: Aggregation Analysis
# ------------------------------------------------------------------------------------

agg_df = filtered_df.groupBy("therapeutic_area").agg(
    F.round(F.avg("ic50"), 2).alias("avg_ic50"),
    F.round(F.avg("auc"), 2).alias("avg_auc"),
    F.round(F.avg("efficacy"), 2).alias("avg_efficacy"),
    F.sum("sample_size").alias("total_sample_size"),
    F.count("study_id").alias("study_count")
)

# ------------------------------------------------------------------------------------
# Section: Join Analysis
# ------------------------------------------------------------------------------------

joined_df = filtered_df.join(
    agg_df,
    on="therapeutic_area",
    how="inner"
)

# ------------------------------------------------------------------------------------
# Section: Result Analysis (overall_score, potential_category)
# ------------------------------------------------------------------------------------

result_df = joined_df.withColumn(
    "overall_score",
    overall_score_udf(
        F.col("score1"),
        F.col("score2"),
        F.col("score3"),
        F.col("score4"),
        F.col("score5")
    )
).withColumn(
    "potential_category",
    potential_category_udf(F.col("overall_score"))
)

# ------------------------------------------------------------------------------------
# Section: Output - Select Columns in Required Order
# ------------------------------------------------------------------------------------

final_df = result_df.select(
    "study_id", "compound_id", "mutation_id", "therapeutic_area", "drug_name",
    "ic50", "auc", "efficacy", "toxicity", "potency", "sample_size",
    "mutation_frequency", "mutation_severity", "compound_concentration",
    "cell_viability", "growth_inhibition", "result", "approved_flag",
    "validation_status", "status", "created_by", "score1", "score2", "score3",
    "score4", "score5", "avg_ic50", "avg_auc", "avg_efficacy",
    "total_sample_size", "study_count", "overall_score", "potential_category"
)

# ------------------------------------------------------------------------------------
# Section: Display Results
# ------------------------------------------------------------------------------------

final_df.show(truncate=False)

# ------------------------------------------------------------------------------------
# End of Compound Drug Analysis Aggregation and Categorization - PySpark Implementation
# ------------------------------------------------------------------------------------

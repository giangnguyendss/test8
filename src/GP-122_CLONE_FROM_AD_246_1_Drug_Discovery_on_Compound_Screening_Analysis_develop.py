# ---------------------------------------------------------------
# PySpark Implementation: Compound Drug Screening Analysis
# Unity Catalog: purgo_databricks
# Schema: purgo_playground
# Table: compound_drug_analysis
# ---------------------------------------------------------------

# from pyspark.sql import SparkSession  # SparkSession is already available in Databricks

# -------------------- Imports --------------------
from pyspark.sql import functions as F  
from pyspark.sql.window import Window  
from pyspark.sql.types import DoubleType, IntegerType, StringType  

# -------------------- Setup: Catalog and Schema --------------------
# spark.catalog.setCurrentCatalog("purgo_databricks")  # Already set in context

# -------------------- CTE: Filtered Compound Data --------------------
filtered_compound_cte = (
    """
        SELECT *
        FROM purgo_playground.compound_drug_analysis
        WHERE approved_flag = '1'
          AND validation_status = 'valid'
    """
)

# -------------------- CTE: Aggregated Metrics by therapeutic_area --------------------
aggregated_metrics_cte = (
    """
        SELECT
            therapeutic_area,
            AVG(ic50) AS avg_ic50,
            AVG(auc) AS avg_auc,
            AVG(efficacy) AS avg_efficacy,
            SUM(sample_size) AS total_sample_size,
            COUNT(*) AS study_count
        FROM purgo_playground.compound_drug_analysis
        WHERE approved_flag = '1'
          AND validation_status = 'valid'
        GROUP BY therapeutic_area
    """
)

# -------------------- Read Filtered Data --------------------
filtered_df = spark.sql("""
    WITH filtered_compound AS (
        SELECT *
        FROM purgo_playground.compound_drug_analysis
        WHERE approved_flag = '1'
          AND validation_status = 'valid'
    )
    SELECT * FROM filtered_compound
""")

# -------------------- Read Aggregated Metrics --------------------
agg_df = spark.sql("""
    WITH aggregated_metrics AS (
        SELECT
            therapeutic_area,
            AVG(ic50) AS avg_ic50,
            AVG(auc) AS avg_auc,
            AVG(efficacy) AS avg_efficacy,
            SUM(sample_size) AS total_sample_size,
            COUNT(*) AS study_count
        FROM purgo_playground.compound_drug_analysis
        WHERE approved_flag = '1'
          AND validation_status = 'valid'
        GROUP BY therapeutic_area
    )
    SELECT * FROM aggregated_metrics
""")

# -------------------- Join Filtered Data with Aggregated Metrics --------------------
# If join columns have the same name, ensure no duplication
# All columns from filtered_df + aggregated metrics
joined_df = filtered_df.join(
    agg_df,
    on="therapeutic_area",
    how="inner"
)

# -------------------- Data Quality Checks --------------------
# Exclude rows with nulls in any required numeric field
required_numeric_cols = [
    "ic50", "auc", "efficacy", "sample_size",
    "score1", "score2", "score3", "score4", "score5"
]
for col_name in required_numeric_cols:
    joined_df = joined_df.filter(F.col(col_name).isNotNull())

# Validate and convert data types before analysis
for col_name in ["ic50", "auc", "efficacy", "score1", "score2", "score3", "score4", "score5"]:
    joined_df = joined_df.withColumn(col_name, F.col(col_name).cast(DoubleType()))
joined_df = joined_df.withColumn("sample_size", F.col("sample_size").cast(IntegerType()))

# -------------------- Result Analysis: overall_score and potential_category --------------------
joined_df = joined_df.withColumn(
    "overall_score",
    (
        F.col("score1") +
        F.col("score2") +
        F.col("score3") +
        F.col("score4") +
        F.col("score5")
    ) / F.lit(5)
)

joined_df = joined_df.withColumn(
    "potential_category",
    F.when((F.col("overall_score") >= 70) & (F.col("overall_score") <= 100), F.lit("High Potential"))
     .when((F.col("overall_score") >= 60) & (F.col("overall_score") < 70), F.lit("Moderate Potential"))
     .when(F.col("overall_score") < 60, F.lit("Low Potential"))
     .otherwise(F.lit("Unknown"))
)

# -------------------- Final Output: Select All Required Columns --------------------
final_columns = [
    "therapeutic_area", "ic50", "auc", "efficacy", "sample_size", "approved_flag", "validation_status",
    "score1", "score2", "score3", "score4", "score5",
    "avg_ic50", "avg_auc", "avg_efficacy", "total_sample_size", "study_count",
    "overall_score", "potential_category"
]
final_df = joined_df.select(*final_columns)

# -------------------- Display Results --------------------
final_df.show(truncate=False)  # Display all columns and result analysis

# -------------------- END OF SCRIPT --------------------

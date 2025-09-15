spark.catalog.setCurrentCatalog("purgo_databricks")

# ------------------------------------------------------------------------------------
# Compound Screening Analysis Implementation
# - Aggregation, Filtering, Joining, and Result Categorization using PySpark DataFrame APIs
# - Unity Catalog: purgo_databricks
# - Schema: purgo_playground
# - Source Table: purgo_playground.compound_drug_analysis
# - All code below is executable in Databricks
# ------------------------------------------------------------------------------------

# from pyspark.sql import SparkSession  # SparkSession is already available in Databricks

# -- Required imports for PySpark DataFrame operations
from pyspark.sql.types import StringType, FloatType, IntegerType  
from pyspark.sql import functions as F  

# ------------------------------------------------------------------------------------
# SECTION: Read Source Table with Error Handling
# - Reads the compound_drug_analysis table from Unity Catalog
# - Handles missing or invalid table gracefully
# ------------------------------------------------------------------------------------
try:
    df_compound = spark.table("purgo_playground.compound_drug_analysis")
except Exception as e:
    # Log error and create empty DataFrame with expected schema
    print(f"Error reading source table: {e}")
    from pyspark.sql.types import StructType, StructField
    schema = StructType([
        StructField("therapeutic_area", StringType(), True),
        StructField("ic50", FloatType(), True),
        StructField("auc", FloatType(), True),
        StructField("efficacy", FloatType(), True),
        StructField("sample_size", IntegerType(), True),
        StructField("approved_flag", StringType(), True),
        StructField("validation_status", StringType(), True),
        StructField("score1", FloatType(), True),
        StructField("score2", FloatType(), True),
        StructField("score3", FloatType(), True),
        StructField("score4", FloatType(), True),
        StructField("score5", FloatType(), True)
    ])
    df_compound = spark.createDataFrame([], schema=schema)

# ------------------------------------------------------------------------------------
# SECTION: Data Type Validation and Conversion
# - Ensures all columns have correct Databricks native data types
# - Casts columns to expected types for schema consistency
# ------------------------------------------------------------------------------------
df_compound = df_compound.select(
    F.col("therapeutic_area").cast(StringType()).alias("therapeutic_area"),
    F.col("ic50").cast(FloatType()).alias("ic50"),
    F.col("auc").cast(FloatType()).alias("auc"),
    F.col("efficacy").cast(FloatType()).alias("efficacy"),
    F.col("sample_size").cast(IntegerType()).alias("sample_size"),
    F.col("approved_flag").cast(StringType()).alias("approved_flag"),
    F.col("validation_status").cast(StringType()).alias("validation_status"),
    F.col("score1").cast(FloatType()).alias("score1"),
    F.col("score2").cast(FloatType()).alias("score2"),
    F.col("score3").cast(FloatType()).alias("score3"),
    F.col("score4").cast(FloatType()).alias("score4"),
    F.col("score5").cast(FloatType()).alias("score5")
)

# ------------------------------------------------------------------------------------
# SECTION: CTE - Aggregated Data by therapeutic_area
# - Groups by therapeutic_area
# - Calculates avg(ic50), avg(auc), avg(efficacy), sum(sample_size), count of studies
# ------------------------------------------------------------------------------------
df_agg_cte = (
    df_compound
    .groupBy("therapeutic_area")
    .agg(
        F.avg("ic50").alias("avg_ic50"),
        F.avg("auc").alias("avg_auc"),
        F.avg("efficacy").alias("avg_efficacy"),
        F.sum("sample_size").alias("total_sample_size"),
        F.count("*").alias("study_count")
    )
)

# ------------------------------------------------------------------------------------
# SECTION: CTE - Filtered Data
# - Filters rows where approved_flag = '1' and validation_status = 'valid'
# ------------------------------------------------------------------------------------
df_filtered_cte = (
    df_compound
    .filter(
        (F.col("approved_flag") == "1") &
        (F.col("validation_status") == "valid")
    )
)

# ------------------------------------------------------------------------------------
# SECTION: CTE - Join Filtered Data with Aggregated Data
# - Joins filtered data with aggregated data on therapeutic_area
# - Ensures only matching therapeutic_area rows are included
# ------------------------------------------------------------------------------------
df_joined_cte = (
    df_filtered_cte
    .join(df_agg_cte, on="therapeutic_area", how="inner")
)

# ------------------------------------------------------------------------------------
# SECTION: CTE - Result Analysis
# - Computes overall_score as average of score1-5
# - Categorizes results into High, Moderate, Low Potential
# - Handles NULLs and NaNs in score columns
# ------------------------------------------------------------------------------------
score_cols = ["score1", "score2", "score3", "score4", "score5"]

# Exclude rows with NULL or NaN in any score column
df_valid_scores_cte = df_joined_cte
for col_name in score_cols:
    df_valid_scores_cte = df_valid_scores_cte.filter(
        F.col(col_name).isNotNull() & ~F.isnan(F.col(col_name))
    )

df_result_cte = (
    df_valid_scores_cte
    .withColumn(
        "overall_score",
        (
            F.col("score1") +
            F.col("score2") +
            F.col("score3") +
            F.col("score4") +
            F.col("score5")
        ) / F.lit(5.0)
    )
    .withColumn(
        "category",
        F.when(
            (F.col("overall_score") >= 70) & (F.col("overall_score") <= 100),
            F.lit("High Potential")
        ).when(
            (F.col("overall_score") >= 60) & (F.col("overall_score") < 70),
            F.lit("Moderate Potential")
        ).when(
            F.col("overall_score") < 60,
            F.lit("Low Potential")
        ).otherwise(F.lit("Unknown"))
    )
)

# ------------------------------------------------------------------------------------
# SECTION: Final Output Display
# - Displays the results with all columns and result analysis
# ------------------------------------------------------------------------------------
df_result_cte.select(
    "therapeutic_area",
    "avg_ic50",
    "avg_auc",
    "avg_efficacy",
    "total_sample_size",
    "study_count",
    "approved_flag",
    "validation_status",
    "score1",
    "score2",
    "score3",
    "score4",
    "score5",
    "overall_score",
    "category"
).show(truncate=False)

# spark.stop()  # Do not stop SparkSession in Databricks

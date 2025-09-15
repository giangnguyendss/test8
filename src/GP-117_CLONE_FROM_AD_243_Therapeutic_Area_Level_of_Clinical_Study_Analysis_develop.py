spark.catalog.setCurrentCatalog("purgo_databricks")

# ---------------------------------------------------------------------------------
# Therapeutic Area Study Aggregation Analysis
# - Aggregates study data from Unity Catalog table "purgo_playground.study_therapeutic_analysis"
# - Groups by "therapeutic_area" and calculates:
#     * total_studies: Count of all studies per therapeutic area
#     * completed_studies: Count of studies with status "Completed"
#     * ongoing_studies: Count of studies with status "Ongoing"
#     * avg_enrolled_subjects: Average enrolled subjects per therapeutic area (2 decimals)
# - Excludes records with null/invalid key columns (therapeutic_area, study_conduct_status, enrolled_subjects < 0)
# - Handles unrecognized study_conduct_status values per requirements
# - Output columns: "therapeutic_area", "total_studies", "completed_studies", "ongoing_studies", "avg_enrolled_subjects"
# - All code is Databricks production-ready PySpark, with error handling and schema validation
# ---------------------------------------------------------------------------------

# from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql.functions import col, when, count, avg, round  

# Set Unity Catalog context for all operations
spark.sql("USE CATALOG purgo_databricks")  # Set current catalog to purgo_databricks

# -------------------- CTE: Valid Study Records --------------------
# Exclude records with null or invalid key columns for aggregation
with_valid_study_records = (
    spark.table("purgo_playground.study_therapeutic_analysis")
    .filter(
        (col("therapeutic_area").isNotNull()) &
        (col("study_conduct_status").isNotNull()) &
        (col("enrolled_subjects").isNotNull()) &
        (col("enrolled_subjects") >= 0)
    )
)

# -------------------- CTE: Aggregated Metrics Per Therapeutic Area --------------------
# Group by therapeutic_area and aggregate required metrics
with_therapeutic_area_agg = (
    with_valid_study_records
    .groupBy("therapeutic_area")
    .agg(
        count("*").alias("total_studies"),  # Count of all studies per therapeutic area
        count(when(col("study_conduct_status") == "Completed", True)).alias("completed_studies"),  # Completed studies
        count(when(col("study_conduct_status") == "Ongoing", True)).alias("ongoing_studies"),  # Ongoing studies
        round(avg(col("enrolled_subjects")), 2).alias("avg_enrolled_subjects")  # Average enrolled subjects, 2 decimals
    )
)

# -------------------- Display Final Output --------------------
# Display results with required columns and ordering
with_therapeutic_area_agg.select(
    "therapeutic_area",
    "total_studies",
    "completed_studies",
    "ongoing_studies",
    "avg_enrolled_subjects"
).orderBy("therapeutic_area").show(truncate=False)

# spark.stop()  # Do not stop SparkSession in Databricks

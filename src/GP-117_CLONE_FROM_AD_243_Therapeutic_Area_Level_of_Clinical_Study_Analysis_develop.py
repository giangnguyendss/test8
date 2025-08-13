# /*
# Databricks PySpark Implementation: Therapeutic Area Study Aggregation and Analysis
#
# This script reads from purgo_playground.study_therapeutic_analysis,
# aggregates study data by therapeutic_area, and writes results to
# purgo_playground.study_therapeutic_analysis_results.
#
# Aggregation logic:
# - Only rows with non-null therapeutic_area and study_title are included
# - total_studies: count of unique study_title per therapeutic_area
# - completed_studies: count of studies with study_conduct_status = "Completed"
# - ongoing_studies: count of studies with study_conduct_status = "Ongoing"
# - avg_enrolled_subjects: average of enrolled_subjects_qty per therapeutic_area, excluding nulls
# - Duplicate study_title within a therapeutic_area is counted once for total_studies
# - Only "Completed" and "Ongoing" statuses are counted for their respective metrics
# - All other statuses are ignored for completed/ongoing counts
# - If all enrolled_subjects_qty are null for a therapeutic_area, avg_enrolled_subjects is null
# - Output table schema: therapeutic_area (STRING), total_studies (BIGINT), completed_studies (BIGINT), ongoing_studies (BIGINT), avg_enrolled_subjects (DOUBLE)
# */

# from pyspark.sql import SparkSession  # SparkSession is already available in Databricks

from pyspark.sql.functions import col, countDistinct, sum as _sum, avg  

# Set current catalog to Unity Catalog 'purgo_databricks'
spark.catalog.setCurrentCatalog("purgo_databricks")

# Read source table from Unity Catalog
try:
    src_df = spark.table("purgo_playground.study_therapeutic_analysis")
except Exception as e:
    raise RuntimeError("Source table purgo_playground.study_therapeutic_analysis not found") from e

# Filter out rows with null therapeutic_area or study_title
filtered_df = src_df.filter(
    col("therapeutic_area").isNotNull() & col("study_title").isNotNull()
)

# CTE: Aggregation logic per requirements
agg_df = (
    filtered_df.groupBy("therapeutic_area")
    .agg(
        countDistinct("study_title").alias("total_studies"),  # Unique study_title per therapeutic_area
        _sum((col("study_conduct_status") == "Completed").cast("int")).cast("bigint").alias("completed_studies"),  # Count where status is Completed
        _sum((col("study_conduct_status") == "Ongoing").cast("int")).cast("bigint").alias("ongoing_studies"),      # Count where status is Ongoing
        avg(col("enrolled_subjects_qty")).alias("avg_enrolled_subjects")  # Average enrolled_subjects_qty, excluding nulls
    )
)

# Display results for validation
agg_df.show()

# Write results to output table in Unity Catalog
try:
    agg_df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable("purgo_playground.study_therapeutic_analysis_results")
except Exception as e:
    raise RuntimeError("Output table purgo_playground.study_therapeutic_analysis_results not found or cannot be written") from e

# spark.stop()  # Do not stop SparkSession in Databricks

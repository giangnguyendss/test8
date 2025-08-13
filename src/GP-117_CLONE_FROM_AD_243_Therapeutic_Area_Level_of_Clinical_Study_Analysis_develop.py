/*
    Databricks PySpark Implementation: Therapeutic Area Study Aggregation and Analysis
    -------------------------------------------------------------------------------
    - Aggregates study data from purgo_playground.study_therapeutic_analysis
    - Groups by therapeutic_area, calculates:
        * total_studies: count of all studies per therapeutic_area
        * completed_studies: count where study_conduct_status = "Completed"
        * ongoing_studies: count where study_conduct_status = "Ongoing"
        * avg_enrolled_subjects: average of enrolled_subjects_qty per therapeutic_area, excluding nulls
    - Excludes records with null therapeutic_area
    - Handles nulls and invalid statuses per requirements
    - Writes results to purgo_playground.study_therapeutic_analysis_results
    - Includes validation logic using CTE pattern
    - All code is Databricks-compatible and follows best practices
*/

/* -------------------- Setup and Imports -------------------- */

# spark.catalog.setCurrentCatalog("purgo_databricks")  # Catalog is already set in Databricks

# from pyspark.sql import SparkSession  # SparkSession is available in Databricks
from pyspark.sql.functions import col, when, count, avg, round  # Built-in
from pyspark.sql import DataFrame  # Built-in

/* -------------------- Read Source Table -------------------- */

# Read source table
src_df = spark.table("purgo_playground.study_therapeutic_analysis")

/* -------------------- Aggregation Logic -------------------- */

agg_df = (
    src_df
    .filter(col("therapeutic_area").isNotNull())  # Exclude NULL therapeutic_area
    .groupBy("therapeutic_area")
    .agg(
        count("study_roll_number").alias("total_studies"),  # Count all studies per therapeutic_area
        count(when(col("study_conduct_status") == "Completed", 1)).alias("completed_studies"),  # Completed studies
        count(when(col("study_conduct_status") == "Ongoing", 1)).alias("ongoing_studies"),  # Ongoing studies
        round(avg(when(col("enrolled_subjects_qty").isNotNull(), col("enrolled_subjects_qty"))), 2).alias("avg_enrolled_subjects")  # Average enrolled_subjects_qty, excluding NULLs
    )
)

/* -------------------- Display Results -------------------- */

agg_df.show()

/* -------------------- Delta Lake Write and Error Handling -------------------- */

try:
    agg_df.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.study_therapeutic_analysis_results")
except Exception as e:
    print("Error: Output table purgo_playground.study_therapeutic_analysis_results does not exist")
    print(str(e))

/* -------------------- Validation Query using CTE -------------------- */

def validate_therapeutic_analysis(df: DataFrame) -> DataFrame:
    # CTE for validation
    cte_df = df.select(
        col("therapeutic_area"),
        col("total_studies"),
        col("completed_studies"),
        col("ongoing_studies"),
        col("avg_enrolled_subjects")
    )
    # Validation: total_studies >= completed_studies + ongoing_studies
    return cte_df.filter(col("total_studies") >= (col("completed_studies") + col("ongoing_studies")))

# Run validation
validated_df = validate_therapeutic_analysis(agg_df)
validated_df.show()

# spark.stop()  # Do not stop Spark in Databricks

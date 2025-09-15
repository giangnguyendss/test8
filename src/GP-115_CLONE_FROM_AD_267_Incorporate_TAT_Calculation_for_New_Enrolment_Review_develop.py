spark.catalog.setCurrentCatalog("purgo_databricks")

# ------------------------------------------------------------------------------
# PySpark Script for TAT Calculation: New Enrollment Review Activity
# Unity Catalog: purgo_databricks
# Schema: purgo_playground
# Input Tables: pat_case, sr_activity
# Output Table: tat_report (all columns from pat_case + total_time_elapsed)
# Logic:
#   - For pat_case records with service_request_type = "Patient Foundation"
#   - Find sr_activity records with subject = "Perform New Enrollment Review Activity" and status = "Completed"
#   - For each case_id, get minimum last_modified_date as end_date
#   - Start date: sr_created_date from pat_case
#   - total_time_elapsed: number of working days (Mon-Fri) between start_date and end_date (date only)
#   - If end_date < start_date OR either is NULL, total_time_elapsed = NULL
#   - If end_date = start_date, total_time_elapsed = 0
#   - Exclude weekends (Saturday/Sunday) from count
#   - Only include pat_case records with matching sr_activity (inner join)
#   - Overwrite tat_report table on each run
#   - All code is Databricks-compatible and follows best practices
# ------------------------------------------------------------------------------

# Commented out SparkSession import and initialization (already available in Databricks)
# from pyspark.sql import SparkSession  # built-in
# spark = SparkSession.builder.getOrCreate()  # built-in

# Import required PySpark functions and types
from pyspark.sql import functions as F  
from pyspark.sql.types import IntegerType  
from pyspark.sql.window import Window  

# ------------------------------------------------------------------------------
# Helper UDF: Calculate number of working days (Mon-Fri) between two dates
# Returns:
#   - Integer (number of working days)
#   - NULL if either date is NULL or end < start
#   - 0 if end = start
# ------------------------------------------------------------------------------

@F.udf(returnType=IntegerType())
def working_days_udf(start_ts, end_ts):
    # start_ts, end_ts: TimestampType
    if start_ts is None or end_ts is None:
        return None
    start_date = start_ts.date()
    end_date = end_ts.date()
    if end_date < start_date:
        return None
    if end_date == start_date:
        return 0
    from datetime import timedelta
    days = 0
    current = start_date
    while current < end_date:
        if current.weekday() < 5:  # 0=Mon, ..., 4=Fri
            days += 1
        current += timedelta(days=1)
    return days

# ------------------------------------------------------------------------------
# CTE: Filtered sr_activity for subject/status match
# ------------------------------------------------------------------------------

filtered_sr_activity_cte = (
    spark.table("purgo_databricks.purgo_playground.sr_activity")
    .filter(
        (F.col("subject") == "Perform New Enrollment Review Activity") &
        (F.col("status") == "Completed")
    )
)

# ------------------------------------------------------------------------------
# CTE: For each case_id, get minimum last_modified_date as end_date
# ------------------------------------------------------------------------------

min_last_modified_window = Window.partitionBy("case_id").orderBy(F.col("last_modified_date").asc())

sr_activity_min_cte = (
    filtered_sr_activity_cte
    .withColumn("min_last_modified_date", F.min("last_modified_date").over(min_last_modified_window))
    .groupBy("case_id")
    .agg(F.min("min_last_modified_date").alias("end_date"))
)

# ------------------------------------------------------------------------------
# CTE: Filter pat_case for service_request_type match
# ------------------------------------------------------------------------------

pat_case_filtered_cte = (
    spark.table("purgo_databricks.purgo_playground.pat_case")
    .filter(F.col("service_request_type") == "Patient Foundation")
)

# ------------------------------------------------------------------------------
# Join pat_case with sr_activity_min on case_id
# Calculate total_time_elapsed using working_days_udf
# ------------------------------------------------------------------------------

tat_joined_df = (
    pat_case_filtered_cte
    .join(sr_activity_min_cte, on="case_id", how="inner")
    .withColumn("total_time_elapsed", working_days_udf(F.col("sr_created_date"), F.col("end_date")))
)

# ------------------------------------------------------------------------------
# Select all columns from pat_case plus total_time_elapsed
# Ensure column order and types match target schema
# ------------------------------------------------------------------------------

pat_case_columns = [field.name for field in pat_case_filtered_cte.schema.fields]
tat_report_df = tat_joined_df.select(
    *pat_case_columns,
    F.col("total_time_elapsed")
)

# ------------------------------------------------------------------------------
# Write tat_report to Unity Catalog (overwrite, Delta Lake)
# ------------------------------------------------------------------------------

tat_report_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("purgo_databricks.purgo_playground.tat_report")

# -------------------------------------------------------------------------------
# End of script
# -------------------------------------------------------------------------------

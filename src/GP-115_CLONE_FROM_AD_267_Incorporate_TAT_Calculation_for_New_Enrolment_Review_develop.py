spark.catalog.setCurrentCatalog("purgo_databricks")

# -----------------------------------------------------------------------------
# PySpark Script: Calculate New Enrollment Review TAT and Generate tat_report
# Unity Catalog: purgo_databricks
# Schema: purgo_playground
# Source Tables: pat_case, sr_activity
# Output Table: tat_report
# 
# Logic:
#   - For each pat_case with service_request_type = "Patient Foundation":
#       - Find related sr_activity records with subject = "Perform New Enrollment Review Activity" and status = "Completed"
#       - Start date: sr_created_date (date only)
#       - End date: minimum last_modified_date (date only) for the case_id
#       - total_time_elapsed:
#           - NULL if either date is NULL or end < start
#           - 0 if end = start
#           - Otherwise, count of business days (Mon-Fri) between start and end, excluding weekends
#   - Output: All columns from pat_case plus total_time_elapsed (nullable integer)
#   - Write to tat_report table in overwrite mode
# -----------------------------------------------------------------------------

# -- All necessary imports (no SparkSession initialization needed in Databricks)
from pyspark.sql import functions as F  
from pyspark.sql.types import DateType, IntegerType  
from datetime import datetime, timedelta  

# -----------------------------------------------------------------------------
# CTE: pat_case_with_dates
#   - Read pat_case table
#   - Filter for service_request_type = "Patient Foundation"
#   - Cast sr_created_date to DateType (NULL if invalid)
# -----------------------------------------------------------------------------
pat_case_with_dates = (
    spark.sql("""
        SELECT
            *,
            TRY_CAST(sr_created_date AS DATE) AS sr_created_date_dt
        FROM purgo_databricks.purgo_playground.pat_case
        WHERE service_request_type = 'Patient Foundation'
    """)
)

# -----------------------------------------------------------------------------
# CTE: sr_activity_filtered
#   - Read sr_activity table
#   - Filter for subject and status
#   - Cast last_modified_date to DateType (NULL if invalid)
# -----------------------------------------------------------------------------
sr_activity_filtered = (
    spark.sql("""
        SELECT
            case_id,
            TRY_CAST(last_modified_date AS DATE) AS last_modified_date_dt
        FROM purgo_databricks.purgo_playground.sr_activity
        WHERE subject = 'Perform New Enrollment Review Activity'
          AND status = 'Completed'
    """)
)

# -----------------------------------------------------------------------------
# CTE: min_activity_per_case
#   - For each case_id, get minimum last_modified_date_dt
# -----------------------------------------------------------------------------
min_activity_per_case = (
    sr_activity_filtered
    .groupBy("case_id")
    .agg(F.min("last_modified_date_dt").alias("min_last_modified_date_dt"))
)

# -----------------------------------------------------------------------------
# CTE: joined_cases
#   - Join pat_case_with_dates with min_activity_per_case on case_id (left join)
# -----------------------------------------------------------------------------
joined_cases = (
    pat_case_with_dates
    .join(min_activity_per_case, on="case_id", how="left")
)

# -----------------------------------------------------------------------------
# UDF: business_days_between
#   - Count business days (Mon-Fri) between start and end dates, inclusive
#   - Exclude weekends (Saturday, Sunday)
#   - Exclude start date from count (per requirements)
# -----------------------------------------------------------------------------
def business_days_between(start, end):
    if start is None or end is None:
        return None
    if end < start:
        return None
    if end == start:
        return 0
    day_count = 0
    current = start
    while current <= end:
        if current.weekday() < 5:  # 0=Monday, 4=Friday
            day_count += 1
        current += timedelta(days=1)
    return day_count - 1  # Exclude start date

business_days_between_udf = F.udf(business_days_between, IntegerType())

# -----------------------------------------------------------------------------
# CTE: tat_report_final
#   - Calculate total_time_elapsed per business rules
#   - Select all columns from pat_case plus total_time_elapsed
# -----------------------------------------------------------------------------
tat_report_final = (
    joined_cases
    .withColumn(
        "total_time_elapsed",
        business_days_between_udf(F.col("sr_created_date_dt"), F.col("min_last_modified_date_dt"))
    )
    # Ensure output columns: all from pat_case plus total_time_elapsed
    .select(
        *[col for col in pat_case_with_dates.columns if col not in ("sr_created_date_dt")],
        "total_time_elapsed"
    )
)

# -----------------------------------------------------------------------------
# Write tat_report_final to Delta table (overwrite mode)
# -----------------------------------------------------------------------------
tat_report_final.write.mode("overwrite").format("delta").saveAsTable("purgo_databricks.purgo_playground.tat_report")

# -----------------------------------------------------------------------------
# End of script
# -----------------------------------------------------------------------------

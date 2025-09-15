# -----------------------------------------------------------------------------------
# HCP Efficient Analysis for Sales per Minute and Conversion Rate
# -----------------------------------------------------------------------------------
# Catalog: purgo_databricks
# Schema: purgo_playground
# Source Table: hcp_efficient_analysis
# Output: HCP-wise high performance Sales per minute and related metrics
# -----------------------------------------------------------------------------------
# This script reads the source table, validates schema and data, calculates
# Revenue_Converted_Flag, aggregates HCP performance metrics, and outputs
# the results as a DataFrame. All error handling, data quality checks, and
# transformations are performed using Databricks best practices.
# -----------------------------------------------------------------------------------

# Commented out SparkSession initialization (already available in Databricks)
# from pyspark.sql import SparkSession  # built-in
# spark = SparkSession.builder.getOrCreate()

# Required imports for PySpark DataFrame operations
from pyspark.sql.types import StringType, FloatType, IntegerType  
from pyspark.sql.functions import col, when, sum as _sum, count as _count, lit, round  

# -----------------------------------------------------------------------------------
# Step 1: Set Current Catalog (Unity Catalog)
# -----------------------------------------------------------------------------------
spark.catalog.setCurrentCatalog("purgo_databricks")  # Set catalog to purgo_databricks

# -----------------------------------------------------------------------------------
# Step 2: Read Source Table with Data Quality Checks
# -----------------------------------------------------------------------------------
source_table = "purgo_playground.hcp_efficient_analysis"

try:
    # Read source table
    df = spark.table(source_table)
except Exception as e:
    # Handle missing table or read error
    raise RuntimeError(f"Error reading source table '{source_table}': {str(e)}")

# -----------------------------------------------------------------------------------
# Step 3: Validate Required Columns Exist
# -----------------------------------------------------------------------------------
required_columns = [
    "HCP_ID",
    "Revenue_Converted",
    "Interaction_Type",
    "Duration_Minutes",
    "Sales_Amount",
    "Follow_Up_Count"
]
missing_columns = [c for c in required_columns if c not in df.columns]
if missing_columns:
    raise ValueError(f"Column(s) {missing_columns} not found in source table")

# -----------------------------------------------------------------------------------
# Step 4: Data Quality Checks for Valid Values
# -----------------------------------------------------------------------------------
# Check for invalid Revenue_Converted values
invalid_revenue_df = df.filter(~col("Revenue_Converted").isin(["Yes", "No"]) & col("Revenue_Converted").isNotNull())
if invalid_revenue_df.count() > 0:
    invalid_values = [row["Revenue_Converted"] for row in invalid_revenue_df.select("Revenue_Converted").distinct().collect()]
    raise ValueError(f"Invalid value(s) {invalid_values} in Revenue_Converted column. Expected 'Yes' or 'No'.")

# Check for null or negative Duration_Minutes
null_or_negative_duration_df = df.filter((col("Duration_Minutes").isNull()) | (col("Duration_Minutes") < 0))
if null_or_negative_duration_df.count() > 0:
    bad_rows = null_or_negative_duration_df.select("HCP_ID", "Duration_Minutes").collect()
    for row in bad_rows:
        raise ValueError(f"Duration_Minutes contains invalid value {row['Duration_Minutes']} for HCP_ID {row['HCP_ID']}. Must be non-null and non-negative.")

# Check for null or negative Sales_Amount
null_or_negative_sales_df = df.filter((col("Sales_Amount").isNull()) | (col("Sales_Amount") < 0))
if null_or_negative_sales_df.count() > 0:
    bad_rows = null_or_negative_sales_df.select("HCP_ID", "Sales_Amount").collect()
    for row in bad_rows:
        raise ValueError(f"Sales_Amount contains invalid value {row['Sales_Amount']} for HCP_ID {row['HCP_ID']}. Must be non-null and non-negative.")

# -----------------------------------------------------------------------------------
# Step 5: Add Revenue_Converted_Flag Column
# -----------------------------------------------------------------------------------
df_flagged = df.withColumn(
    "Revenue_Converted_Flag",
    when(col("Revenue_Converted") == "Yes", lit(1))
    .when(col("Revenue_Converted") == "No", lit(0))
    .otherwise(lit(None))
)

# -----------------------------------------------------------------------------------
# Step 6: Filter Valid Rows for Aggregation
# -----------------------------------------------------------------------------------
df_valid = df_flagged.filter(
    (col("HCP_ID").isNotNull()) &
    (col("Interaction_Type").isNotNull()) &
    (col("Duration_Minutes").isNotNull()) & (col("Duration_Minutes") >= 0) &
    (col("Sales_Amount").isNotNull()) & (col("Sales_Amount") >= 0) &
    (col("Follow_Up_Count").isNotNull())
)

# -----------------------------------------------------------------------------------
# Step 7: Aggregation using CTE for HCP Performance Metrics
# -----------------------------------------------------------------------------------
# Define CTE for aggregation
from pyspark.sql import DataFrame  

def hcp_performance_cte(df: DataFrame) -> DataFrame:
    # Group by HCP_ID and aggregate required metrics
    return df.groupBy("HCP_ID").agg(
        _count("Interaction_Type").cast("int").alias("Interaction_Count"),
        _sum("Duration_Minutes").cast("float").alias("Total_Duration_Minutes"),
        (_sum("Duration_Minutes") / _count("Interaction_Type")).cast("float").alias("Avg_Duration_Per_Interaction"),
        _sum("Follow_Up_Count").cast("int").alias("Total_Follow_Up_Count"),
        _sum("Sales_Amount").cast("float").alias("Total_Sales_Amount"),
        _sum("Revenue_Converted_Flag").cast("int").alias("Revenue_Converted_Interactions"),
        (round((_sum("Revenue_Converted_Flag") / _count("Interaction_Type")) * 100, 2)).cast("float").alias("Revenue_Conversion_Rate_Percent"),
        (round(_sum("Sales_Amount") / _sum("Duration_Minutes"), 2)).cast("float").alias("Sales_Per_Minute")
    )

# Apply CTE for aggregation
hcp_metrics_df = hcp_performance_cte(df_valid)

# -----------------------------------------------------------------------------------
# Step 8: Data Quality Checks for Aggregated Results
# -----------------------------------------------------------------------------------
# Check for division by zero in Sales_Per_Minute
zero_duration_rows = hcp_metrics_df.filter(col("Total_Duration_Minutes") == 0)
if zero_duration_rows.count() > 0:
    ids = [row["HCP_ID"] for row in zero_duration_rows.select("HCP_ID").collect()]
    raise ZeroDivisionError(f"Total_Duration_Minutes is zero for HCP_ID(s) {ids}. Cannot calculate Sales per Minute.")

# Check for division by zero in Revenue Conversion Rate
zero_interaction_rows = hcp_metrics_df.filter(col("Interaction_Count") == 0)
if zero_interaction_rows.count() > 0:
    ids = [row["HCP_ID"] for row in zero_interaction_rows.select("HCP_ID").collect()]
    raise ZeroDivisionError(f"Interaction_Count is zero for HCP_ID(s) {ids}. Cannot calculate Revenue Conversion Rate.")

# -----------------------------------------------------------------------------------
# Step 9: Final Output - Show HCP-wise High Performance Sales per Minute
# -----------------------------------------------------------------------------------
# Order by Sales_Per_Minute descending for high performance
final_df = hcp_metrics_df.orderBy(col("Sales_Per_Minute").desc())

# Show the results
final_df.show(truncate=False)

# -----------------------------------------------------------------------------------
# End of Script
# -----------------------------------------------------------------------------------

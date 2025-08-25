spark.catalog.setCurrentCatalog("purgo_databricks")

# -----------------------------------------------------------------------------------
# Databricks PySpark Script: Compute and Load Metric Columns from Metric Master SQL Queries
# -----------------------------------------------------------------------------------
# This script computes metric columns by executing SQL queries from purgo_playground.metric_master,
# in the order specified by the dependency column, and loads results into target tables or views
# as configured in purgo_playground.metric_config. It includes error handling, audit logging,
# data type validation, and data quality checks.
# -----------------------------------------------------------------------------------
# from pyspark.sql import SparkSession  # SparkSession is already available in Databricks

# ------------------ Imports ------------------
from pyspark.sql.functions import col, lit, current_timestamp, expr  
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType, ShortType  
import traceback  
import datetime  

# ------------------ Helper Functions ------------------

def get_current_db_timestamp():
    # Returns current UTC timestamp in Databricks format
    return datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0000'

def log_audit(spark, metric_template_name, status, start_time, end_time, error_message=None):
    # Logs audit information for metric execution into metric_audit_log_stg
    audit_schema = StructType([
        StructField("metric_template_name", StringType(), True),
        StructField("execution_status", StringType(), True),
        StructField("start_time", StringType(), True),
        StructField("end_time", StringType(), True),
        StructField("error_message", StringType(), True)
    ])
    audit_row = [(metric_template_name, status, start_time, end_time, error_message)]
    audit_df = spark.createDataFrame(audit_row, schema=audit_schema)
    audit_df.write.mode("append").saveAsTable("purgo_playground.metric_audit_log_stg")

def get_metric_master_map(metric_master_df):
    # Returns a dict mapping metric_template_name to row for fast lookup
    return {row.metric_template_name: row for row in metric_master_df.collect() if row.metric_template_name is not None}

def get_metric_config_map(metric_config_df):
    # Returns a dict mapping metric_template_name to row for fast lookup
    return {row.metric_template_name: row for row in metric_config_df.collect() if row.metric_template_name is not None}

def get_table_schema(spark, table_name):
    # Returns a dict mapping column name to data type for a table
    try:
        df = spark.table(table_name)
        return {field.name: field.dataType for field in df.schema.fields}
    except Exception:
        return {}

def safe_execute_sql(spark, sql_query):
    # Safely executes SQL query, returns DataFrame or error
    try:
        df = spark.sql(sql_query)
        return df, None
    except Exception as e:
        return None, str(e)

def safe_join(df1, df2, join_col):
    # Safely performs join, returns DataFrame or error
    try:
        return df1.join(df2, on=join_col, how="left"), None
    except Exception as e:
        return None, str(e)

def validate_data_type(df, col_name, expected_type):
    # Validates that df[col_name] matches expected_type
    schema = {field.name: field.dataType for field in df.schema.fields}
    if col_name not in schema:
        return False, f"Column {col_name} not found"
    actual_type = schema[col_name]
    type_map = {
        "bigint": LongType(),
        "double": DoubleType(),
        "string": StringType()
    }
    if expected_type not in type_map:
        return False, f"Unknown expected type: {expected_type}"
    if type(actual_type) != type(type_map[expected_type]):
        return False, f"DATA_TYPE_MISMATCH: Expected {expected_type} but found {str(actual_type)}"
    return True, None

def validate_column_count(df, expected_count):
    # Validates that df has expected_count columns
    actual_count = len(df.columns)
    return actual_count == expected_count, actual_count

def validate_row_count(df1, df2, join_col):
    # Validates that row counts match on join_col
    count1 = df1.select(join_col).distinct().count()
    count2 = df2.select(join_col).distinct().count()
    return count1 == count2, count1, count2

def get_expected_metric_dtype(metric_data_type):
    # Map config string to PySpark type
    type_map = {
        "bigint": LongType(),
        "double": DoubleType(),
        "string": StringType()
    }
    return type_map.get(metric_data_type, StringType())

def cast_metric_column(df, metric_col, metric_data_type):
    # Casts metric column to expected data type
    dtype = get_expected_metric_dtype(metric_data_type)
    if dtype == LongType():
        return df.withColumn(metric_col, col(metric_col).cast("long"))
    elif dtype == DoubleType():
        return df.withColumn(metric_col, col(metric_col).cast("double"))
    elif dtype == StringType():
        return df.withColumn(metric_col, col(metric_col).cast("string"))
    else:
        return df

def safe_file_open(spark, file_path, file_type="csv", **kwargs):
    # Gracefully handle missing or invalid data file
    try:
        if file_type == "csv":
            df = spark.read.csv(file_path, **kwargs)
        elif file_type == "parquet":
            df = spark.read.parquet(file_path, **kwargs)
        else:
            df = None
        return df, None
    except Exception as e:
        return None, str(e)

# ------------------ Main Processing ------------------

# Load config and master tables
metric_config_df = spark.table("purgo_playground.metric_config")
metric_master_df = spark.table("purgo_playground.metric_master")

# Build lookup maps
metric_master_map = get_metric_master_map(metric_master_df)
metric_config_map = get_metric_config_map(metric_config_df)

# Filter active metrics and sort by dependency
active_metrics = [row for row in metric_config_df.collect() if row.active_indicator == "Y" and row.metric_template_name is not None]
active_metrics_with_dep = []
for row in active_metrics:
    mm_row = metric_master_map.get(row.metric_template_name)
    if mm_row is not None and mm_row.dependency is not None:
        active_metrics_with_dep.append((mm_row.dependency, row.metric_template_name, row))
active_metrics_with_dep.sort()  # ascending order of dependency

# Process each metric in dependency order
for dep, metric_template_name, mc_row in active_metrics_with_dep:
    start_time = get_current_db_timestamp()
    error_message = None
    status = "SUCCESS"
    try:
        # Validate mapping in metric_master
        mm_row = metric_master_map.get(metric_template_name)
        if mm_row is None:
            error_message = f"MAPPING_ERROR: No matching metric_master entry for metric_template_name {metric_template_name}"
            status = "FAILED"
            continue

        # Validate sql_query
        sql_query = mm_row.sql_query
        if not sql_query:
            error_message = f"SQL_EXECUTION_ERROR: sql_query is null for metric {metric_template_name}"
            status = "FAILED"
            continue

        # Validate source_table and primary_key
        source_table = mc_row.source_table
        primary_key = mc_row.primary_key
        target_table = mc_row.target_table
        metric_data_type = mc_row.metric_data_type

        if not source_table or not primary_key or not target_table:
            error_message = f"CONFIG_ERROR: source_table, primary_key, or target_table missing for metric {metric_template_name}"
            status = "FAILED"
            continue

        # Check source table exists
        try:
            source_df = spark.table(source_table)
        except Exception as e:
            error_message = f"JOIN_ERROR: Table not found: {source_table}"
            status = "FAILED"
            continue

        # Check primary_key exists in source table
        if primary_key not in source_df.columns:
            error_message = f"JOIN_ERROR: Primary key column not found: {primary_key}"
            status = "FAILED"
            continue

        # Execute metric SQL query
        metric_df, sql_error = safe_execute_sql(spark, sql_query)
        if sql_error:
            error_message = f"SQL_EXECUTION_ERROR: {sql_error}"
            status = "FAILED"
            continue

        # Check if metric_df has results
        if metric_df.head(1) == []:
            error_message = f"NO_RESULTS: SQL query returned no results for metric {metric_template_name}"
            status = "FAILED"
            continue

        # Validate metric column exists
        metric_col = metric_template_name
        if metric_col not in metric_df.columns:
            # Try to find column with alias
            metric_col_candidates = [c for c in metric_df.columns if c.lower() == metric_col.lower()]
            if metric_col_candidates:
                metric_col = metric_col_candidates[0]
            else:
                error_message = f"RESULT_ERROR: Metric column {metric_col} not found in SQL result"
                status = "FAILED"
                continue

        # Cast metric column to expected data type
        metric_df = cast_metric_column(metric_df, metric_col, metric_data_type)

        # Validate metric column data type
        valid_dtype, dtype_error = validate_data_type(metric_df, metric_col, metric_data_type)
        if not valid_dtype:
            error_message = dtype_error
            status = "FAILED"
            continue

        # Join metric_df with source_df on primary_key
        joined_df, join_error = safe_join(metric_df, source_df, primary_key)
        if join_error:
            error_message = f"JOIN_ERROR: {join_error}"
            status = "FAILED"
            continue

        # If view_name is not null, save as view
        if mm_row.view_name:
            # In Databricks, use createOrReplaceTempView, but here use saveAsTable for persistent view
            view_name = mm_row.view_name
            joined_df.write.mode("overwrite").saveAsTable(f"purgo_playground.{view_name}")
        else:
            # Load metric column into target_table
            # Check target table exists
            try:
                target_df = spark.table(target_table)
            except Exception as e:
                error_message = f"TARGET_TABLE_ERROR: Table not found: {target_table}"
                status = "FAILED"
                continue

            # Validate number of columns before insertion
            # Only update/add metric_col to target_df
            # Prepare update DataFrame: primary_key + metric_col
            update_df = joined_df.select(primary_key, metric_col)
            # Validate update_df schema matches target table for these columns
            target_schema = get_table_schema(spark, target_table)
            if primary_key not in target_schema or metric_col not in target_schema:
                error_message = f"SCHEMA_ERROR: Column mismatch for {primary_key} or {metric_col} in target table"
                status = "FAILED"
                continue

            # Update target table: merge metric_col values by primary_key
            # In Databricks, use Delta MERGE; here, simulate with overwrite
            # Join update_df to target_df, update metric_col
            merged_df = target_df.join(update_df, on=primary_key, how="left") \
                .withColumn(metric_col, expr(f"coalesce({update_df.columns[1]}, `{target_table}`.`{metric_col}`)"))
            # Ensure column order matches target table
            merged_df = merged_df.select(*target_df.columns)
            merged_df.write.mode("overwrite").saveAsTable(target_table)

        # Data quality checks: row count validation
        valid_row_count, src_count, tgt_count = validate_row_count(source_df, joined_df, primary_key)
        # Log validation result in metric_minus_validation_stg
        validation_schema = StructType([
            StructField("metric_id", StringType(), True),
            StructField("no_of_metric_col", IntegerType(), True),
            StructField("no_of_source_col", LongType(), True),
            StructField("no_of_target_col", LongType(), True),
            StructField("result", StringType(), True)
        ])
        validation_row = [(mc_row.metric_id, 1, src_count, tgt_count, "PASS" if valid_row_count else "FAIL")]
        validation_df = spark.createDataFrame(validation_row, schema=validation_schema)
        validation_df.write.mode("append").saveAsTable("purgo_playground.metric_minus_validation_stg")

    except Exception as e:
        error_message = f"UNEXPECTED_ERROR: {str(e)}"
        status = "FAILED"
    finally:
        end_time = get_current_db_timestamp()
        log_audit(spark, metric_template_name, status, start_time, end_time, error_message)

# ------------------ Process Inactive Metrics ------------------
inactive_metrics = [row for row in metric_config_df.collect() if row.active_indicator == "N" and row.metric_template_name is not None]
for row in inactive_metrics:
    start_time = get_current_db_timestamp()
    end_time = get_current_db_timestamp()
    log_audit(spark, row.metric_template_name, "SKIPPED", start_time, end_time, None)

# ------------------ File Opening Example ------------------
file_path = "purgo_playground/test_file.csv"
df, file_error = safe_file_open(spark, file_path, file_type="csv", header=True)
if file_error:
    # Handle missing or invalid file gracefully
    print(f"File read error: {file_error}")

# ------------------ End of Script ------------------
# spark.stop()  # Do not stop SparkSession in Databricks

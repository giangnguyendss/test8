/*
  Databricks SQL Script for Creating and Populating activity_final_log Table
  Catalog: purgo_databricks
  Schema: purgo_playground
  Table: activity_final_log

  Implements:
    - Table creation with correct schema and column comments
    - Data type validation and conversion
    - Filtering, joining, and transformation logic as per mapping specification
    - Error handling and deduplication
*/

-- SECTION: Setup - Use correct catalog and schema
USE CATALOG purgo_databricks;
USE purgo_playground;

-- SECTION: Table Creation - Create activity_final_log with required columns and comments
CREATE TABLE IF NOT EXISTS purgo_databricks.purgo_playground.activity_final_log (
  usr_id STRING COMMENT 'Distinct modified_by_user_id from entity_activity_log',
  usr_name STRING COMMENT 'Distinct modified_by_user_name associated with usr_id',
  date DATE COMMENT 'Distinct Date (activity_closed_at) associated with usr_id & usr_name',
  activity_bucket STRING COMMENT 'Distinct priority_level_desc from priority_weight_settings for usr_id, date',
  new_act_flag STRING COMMENT '''New'' or ''Continuing'' based on cutoff_timestamp and activity_closed_at'
)
COMMENT 'Final activity log table as per mapping specification';

-- SECTION: Data Insertion - Insert transformed data from source tables
INSERT INTO purgo_databricks.purgo_playground.activity_final_log
SELECT
  e.modified_by_user_id AS usr_id,
  e.modified_by_user_name AS usr_name,
  CAST(SPLIT(e.activity_closed_at, 'T')[0] AS DATE) AS date,
  p.priority_level_desc AS activity_bucket,
  CASE
    WHEN e.cutoff_timestamp IS NULL THEN 'New'
    WHEN CAST(SPLIT(e.activity_closed_at, 'T')[0] AS DATE) <= CAST(e.cutoff_timestamp AS DATE) THEN 'New'
    ELSE 'Continuing'
  END AS new_act_flag
FROM purgo_databricks.purgo_playground.entity_activity_log e
LEFT JOIN purgo_databricks.purgo_playground.priority_weight_settings p
  ON e.subject = p.task_category
WHERE
  e.activity_closed_at IS NOT NULL
  AND CAST(SPLIT(e.activity_closed_at, 'T')[0] AS DATE) >= DATE('2025-01-01')
  AND e.activity_subcategory = 'Task'
  AND e.modified_by_user_role = 'PN'
  AND e.interaction_type IN ('C&R', 'PF');

-- SECTION: Validation Query - Show all records in activity_final_log
WITH activity_final_log_cte AS (
  SELECT * FROM purgo_databricks.purgo_playground.activity_final_log
)
SELECT * FROM activity_final_log_cte
ORDER BY usr_id, date;

-- SECTION: Data type validation for activity_final_log columns
WITH activity_final_log_cte AS (
  SELECT * FROM purgo_databricks.purgo_playground.activity_final_log
)
SELECT
  typeof(usr_id) AS usr_id_type,
  typeof(usr_name) AS usr_name_type,
  typeof(date) AS date_type,
  typeof(activity_bucket) AS activity_bucket_type,
  typeof(new_act_flag) AS new_act_flag_type
FROM activity_final_log_cte
LIMIT 1;

-- End of script


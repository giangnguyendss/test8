/*
  Databricks SQL Script for Creation and Population of activity_final_log Table
  Catalog: purgo_databricks
  Schema: purgo_playground

  Implements mapping specification from "Final Act Log" sheet:
    - Only include records where activity_closed_at >= '2025-01-01' and not NULL
    - Only include activity_subcategory = 'Task'
    - Only include modified_by_user_role = 'PN'
    - Only include interaction_type IN ('C&R', 'PF')
    - activity_bucket: left join entity_activity_log.subject = priority_weight_settings.task_category, select priority_level_desc
    - new_act_flag: if cutoff_timestamp is NULL then 'New', else if activity_closed_at <= cutoff_timestamp then 'New', else 'Continuing'
    - Deduplicate usr_id, usr_name, date
    - Error logging for all mapping violations and edge cases
    - If required columns missing in target table, log error and do not insert
*/

/* ------------------ Setup: Ensure Target and Error Tables Exist ------------------ */

USE CATALOG purgo_databricks;

-- Drop and create target table with required columns
DROP TABLE IF EXISTS purgo_playground.activity_final_log;
CREATE TABLE IF NOT EXISTS purgo_playground.activity_final_log (
  usr_id STRING NOT NULL,
  usr_name STRING NOT NULL,
  date DATE NOT NULL,
  activity_bucket STRING NOT NULL,
  new_act_flag STRING NOT NULL
);

-- Drop and create error table for logging
DROP TABLE IF EXISTS purgo_playground.activity_final_log_errors;
CREATE TABLE IF NOT EXISTS purgo_playground.activity_final_log_errors (
  usr_id STRING,
  usr_name STRING,
  date DATE,
  activity_bucket STRING,
  new_act_flag STRING,
  error_message STRING,
  error_timestamp TIMESTAMP
);

/* ------------------ Main Population Logic with Error Logging ------------------ */

/*
  CTE: Valid Records for activity_final_log
  - Apply all mapping filters
  - Join for activity_bucket
  - Deduplicate usr_id, usr_name, date
  - new_act_flag logic
*/
WITH valid_activity AS (
  SELECT
    e.modified_by_user_id AS usr_id,
    e.modified_by_user_name AS usr_name,
    e.activity_closed_at AS date,
    p.priority_level_desc AS activity_bucket,
    CASE
      WHEN e.cutoff_timestamp IS NULL THEN 'New'
      WHEN e.activity_closed_at <= CAST(e.cutoff_timestamp AS DATE) THEN 'New'
      ELSE 'Continuing'
    END AS new_act_flag,
    ROW_NUMBER() OVER (
      PARTITION BY e.modified_by_user_id, e.modified_by_user_name, e.activity_closed_at
      ORDER BY e.modified_by_user_id, e.modified_by_user_name, e.activity_closed_at
    ) AS rn
  FROM purgo_playground.entity_activity_log e
  LEFT JOIN purgo_playground.priority_weight_settings p
    ON e.subject = p.task_category
  WHERE
    e.activity_closed_at >= DATE('2025-01-01')
    AND e.activity_closed_at IS NOT NULL
    AND e.activity_subcategory = 'Task'
    AND e.modified_by_user_role = 'PN'
    AND e.interaction_type IN ('C&R', 'PF')
    AND e.modified_by_user_id IS NOT NULL
    AND e.modified_by_user_name IS NOT NULL
    AND p.priority_level_desc IS NOT NULL
),
deduped_activity AS (
  SELECT
    usr_id, usr_name, date, activity_bucket, new_act_flag
  FROM valid_activity
  WHERE rn = 1
)

/*
  Insert Valid Records into activity_final_log
  - Only insert if all required columns are present
*/
INSERT INTO purgo_playground.activity_final_log (usr_id, usr_name, date, activity_bucket, new_act_flag)
SELECT
  usr_id, usr_name, date, activity_bucket, new_act_flag
FROM deduped_activity;

/*
  CTE: Error Detection for Mapping Specification Violations
  - Each error scenario is handled and logged with appropriate error_message
*/
WITH error_activity AS (
  SELECT
    e.modified_by_user_id AS usr_id,
    e.modified_by_user_name AS usr_name,
    e.activity_closed_at AS date,
    p.priority_level_desc AS activity_bucket,
    CASE
      WHEN e.cutoff_timestamp IS NULL THEN 'New'
      WHEN e.activity_closed_at <= CAST(e.cutoff_timestamp AS DATE) THEN 'New'
      ELSE 'Continuing'
    END AS new_act_flag,
    CASE
      WHEN e.activity_closed_at IS NULL THEN 'activity_closed_at is NULL'
      WHEN e.activity_closed_at < DATE('2025-01-01') THEN 'activity_closed_at before Jan 2025'
      WHEN e.activity_subcategory IS NULL THEN 'activity_subcategory is NULL'
      WHEN e.activity_subcategory <> 'Task' THEN 'activity_subcategory not ''Task'''
      WHEN e.modified_by_user_role IS NULL THEN 'modified_by_user_role is NULL'
      WHEN e.modified_by_user_role <> 'PN' THEN 'modified_by_user_role not ''PN'''
      WHEN e.interaction_type IS NULL THEN 'interaction_type is NULL'
      WHEN e.interaction_type NOT IN ('C&R', 'PF') THEN 'interaction_type not in (''C&R'', ''PF'')'
      WHEN e.subject IS NULL THEN 'subject is NULL'
      WHEN p.priority_level_desc IS NULL THEN 'No matching priority_level_desc for subject'
      WHEN e.modified_by_user_id IS NULL THEN 'modified_by_user_id is NULL'
      WHEN e.modified_by_user_name IS NULL THEN 'modified_by_user_name is NULL'
      ELSE NULL
    END AS error_message
  FROM purgo_playground.entity_activity_log e
  LEFT JOIN purgo_playground.priority_weight_settings p
    ON e.subject = p.task_category
  WHERE
    -- Only log errors for records not inserted into activity_final_log
    (
      e.activity_closed_at IS NULL
      OR e.activity_closed_at < DATE('2025-01-01')
      OR e.activity_subcategory IS NULL
      OR e.activity_subcategory <> 'Task'
      OR e.modified_by_user_role IS NULL
      OR e.modified_by_user_role <> 'PN'
      OR e.interaction_type IS NULL
      OR e.interaction_type NOT IN ('C&R', 'PF')
      OR e.subject IS NULL
      OR p.priority_level_desc IS NULL
      OR e.modified_by_user_id IS NULL
      OR e.modified_by_user_name IS NULL
    )
),
duplicate_activity AS (
  SELECT
    e.modified_by_user_id AS usr_id,
    e.modified_by_user_name AS usr_name,
    e.activity_closed_at AS date,
    p.priority_level_desc AS activity_bucket,
    CASE
      WHEN e.cutoff_timestamp IS NULL THEN 'New'
      WHEN e.activity_closed_at <= CAST(e.cutoff_timestamp AS DATE) THEN 'New'
      ELSE 'Continuing'
    END AS new_act_flag,
    'Duplicate usr_id, usr_name, date combination' AS error_message
  FROM purgo_playground.entity_activity_log e
  LEFT JOIN purgo_playground.priority_weight_settings p
    ON e.subject = p.task_category
  WHERE
    e.activity_closed_at >= DATE('2025-01-01')
    AND e.activity_closed_at IS NOT NULL
    AND e.activity_subcategory = 'Task'
    AND e.modified_by_user_role = 'PN'
    AND e.interaction_type IN ('C&R', 'PF')
    AND e.modified_by_user_id IS NOT NULL
    AND e.modified_by_user_name IS NOT NULL
    AND p.priority_level_desc IS NOT NULL
  GROUP BY e.modified_by_user_id, e.modified_by_user_name, e.activity_closed_at, p.priority_level_desc, e.cutoff_timestamp
  HAVING COUNT(*) > 1
),
missing_column_error AS (
  SELECT
    NULL AS usr_id,
    NULL AS usr_name,
    NULL AS date,
    NULL AS activity_bucket,
    NULL AS new_act_flag,
    'Target table missing required columns' AS error_message
  WHERE NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_catalog = 'purgo_databricks'
      AND table_schema = 'purgo_playground'
      AND table_name = 'activity_final_log'
      AND column_name IN ('usr_id', 'usr_name', 'date', 'activity_bucket', 'new_act_flag')
    GROUP BY table_name
    HAVING COUNT(*) = 5
  )
)

/*
  Insert Error Records into activity_final_log_errors
*/
INSERT INTO purgo_playground.activity_final_log_errors (
  usr_id, usr_name, date, activity_bucket, new_act_flag, error_message, error_timestamp
)
SELECT
  usr_id, usr_name, date, activity_bucket, new_act_flag, error_message, CURRENT_TIMESTAMP()
FROM error_activity
WHERE error_message IS NOT NULL
UNION ALL
SELECT
  usr_id, usr_name, date, activity_bucket, new_act_flag, error_message, CURRENT_TIMESTAMP()
FROM duplicate_activity
UNION ALL
SELECT
  usr_id, usr_name, date, activity_bucket, new_act_flag, error_message, CURRENT_TIMESTAMP()
FROM missing_column_error;

/* ------------------ End of Script ------------------ */


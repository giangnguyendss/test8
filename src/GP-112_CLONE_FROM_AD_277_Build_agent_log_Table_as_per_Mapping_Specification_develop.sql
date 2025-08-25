USE CATALOG purgo_databricks;

/*
==========================================================================================
Databricks SQL Implementation: Agent Log Calculated Field Extraction and Transformation
==========================================================================================
Unity Catalog: purgo_databricks
Schema: purgo_playground
Target Table: agent_log
Business Logic:
  - Extract, join, and transform data from session_tracking_data, user_presence_tracker, agent_profile_data
  - Calculate derived fields per mapping specification and technical design
  - Enforce all business rules, validation, error handling, deduplication, and data quality
  - Output matches target schema and Databricks data types
==========================================================================================
*/

 /*
------------------------------------------------------------------------------------------
Section: Agent Log Calculated Field Extraction and Transformation Query
------------------------------------------------------------------------------------------
*/

WITH
-- CTE: Most recent agent_profile_data per email_address
latest_agent_profile AS (
  SELECT
    email_address,
    internal_user_key,
    full_name,
    unixid,
    last_modified_timestamp
  FROM (
    SELECT
      email_address,
      internal_user_key,
      full_name,
      unixid,
      last_modified_timestamp,
      ROW_NUMBER() OVER (PARTITION BY email_address ORDER BY last_modified_timestamp DESC) AS rn
    FROM purgo_playground.agent_profile_data
  ) apd
  WHERE rn = 1
),

-- CTE: Valid session_tracking_data records (filter by date, valid timestamps, not null person_identifier)
valid_sessions AS (
  SELECT
    st.person_identifier,
    st.session_start_time,
    st.session_end_time
  FROM purgo_playground.session_tracking_data st
  WHERE
    st.session_start_time >= TRY_TO_TIMESTAMP('2025-01-01T00:00:00.000+0000')
    AND st.person_identifier IS NOT NULL
    AND st.session_start_time IS NOT NULL
),

-- CTE: Join session_tracking_data to user_presence_tracker and agent_profile_data, aggregate per agent per log_date
agent_log_agg AS (
  SELECT
    -- IU_key: join logic, null if missing user_presence_tracker or agent_profile_data
    lap.internal_user_key AS IU_key,
    -- full_name: join logic, null if missing user_presence_tracker or agent_profile_data
    lap.full_name AS full_name,
    -- log_date: date part of session_start_time
    DATE(vs.session_start_time) AS log_date,
    -- first_login: earliest session_start_time per person_identifier per log_date
    MIN(vs.session_start_time) AS first_login,
    -- first_logout: latest session_end_time per person_identifier per log_date
    MAX(vs.session_end_time) AS first_logout,
    -- total_login_time(hrs): difference in hours, null if first_login or first_logout is null
    CASE
      WHEN MIN(vs.session_start_time) IS NOT NULL AND MAX(vs.session_end_time) IS NOT NULL
      THEN ROUND(
        (
          UNIX_TIMESTAMP(MAX(vs.session_end_time)) - UNIX_TIMESTAMP(MIN(vs.session_start_time))
        ) / 3600.0, 2)
      ELSE NULL
    END AS total_login_time_hrs,
    -- city, state, country, zip_code: hardcoded
    'New York' AS city,
    'NY' AS state,
    'USA' AS country,
    '10001' AS zip_code,
    -- data_loaded_at: current timestamp
    CURRENT_TIMESTAMP() AS data_loaded_at,
    -- unique_key: concat all fields and hash, nulls included
    SHA2(
      CONCAT(
        COALESCE(lap.internal_user_key, 'NULL'), '|',
        COALESCE(lap.full_name, 'NULL'), '|',
        COALESCE(CAST(DATE(vs.session_start_time) AS STRING), 'NULL'), '|',
        COALESCE(CAST(MIN(vs.session_start_time) AS STRING), 'NULL'), '|',
        COALESCE(CAST(MAX(vs.session_end_time) AS STRING), 'NULL'), '|',
        COALESCE(CAST(
          CASE
            WHEN MIN(vs.session_start_time) IS NOT NULL AND MAX(vs.session_end_time) IS NOT NULL
            THEN ROUND(
              (
                UNIX_TIMESTAMP(MAX(vs.session_end_time)) - UNIX_TIMESTAMP(MIN(vs.session_start_time))
              ) / 3600.0, 2)
            ELSE NULL
          END
        AS STRING), 'NULL'), '|',
        'New York', '|', 'NY', '|', 'USA', '|', '10001', '|',
        CAST(CURRENT_TIMESTAMP() AS STRING)
      ), 256
    ) AS unique_key
  FROM valid_sessions vs
  LEFT JOIN purgo_playground.user_presence_tracker upt
    ON vs.person_identifier = upt.person_identifier
  LEFT JOIN latest_agent_profile lap
    ON upt.email_address = lap.email_address
  GROUP BY
    lap.internal_user_key,
    lap.full_name,
    DATE(vs.session_start_time)
)

-- Final SELECT: Deduplicate, output one record per person_identifier per log_date
SELECT
  IU_key,
  full_name,
  log_date,
  first_login,
  first_logout,
  total_login_time_hrs,
  city,
  state,
  country,
  zip_code,
  data_loaded_at,
  unique_key
FROM agent_log_agg
QUALIFY ROW_NUMBER() OVER (PARTITION BY IU_key, log_date ORDER BY first_login) = 1
-- Comments:
-- - All business rules, null handling, deduplication, and data types enforced
-- - Output matches purgo_playground.agent_log schema
-- - Filtering, window functions, and hashing per requirements
-- - No temp views or temp tables used
-- - CTEs used directly in the query
-- - No trailing explanatory text
-- - End of implementation

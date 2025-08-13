USE CATALOG purgo_databricks;

/*==============================================================================
Agent Log Calculated Field Query
==============================================================================
Unity Catalog: purgo_databricks
Schema: purgo_playground

- Joins session_tracking_data, user_presence_tracker, agent_profile_data
- Handles hardcoded city/state/country/zip_code
- Surrogate key agent_key: SHA256(concat(all output fields))
- Error logging to agent_log_error_log for missing/null data
- Window functions for first_login/first_logout per agent per day
- Most recent agent_name/unix_id per email_address (by last_modified_timestamp)
- Filters: log_date range, agent selection (optional via WHERE)
- Output: Display only, no insert/update
==============================================================================*/

/*-----------------------------------------------------------------------------
SECTION: UDF for Surrogate Key Generation
-----------------------------------------------------------------------------*/
CREATE OR REPLACE FUNCTION purgo_playground.agent_log_surrogate_key(
  unix_id STRING,
  agent_name STRING,
  log_date DATE,
  agent_first_login TIMESTAMP,
  agent_first_logout TIMESTAMP,
  total_login_time_hrs STRING,
  agent_lunch_login TIMESTAMP,
  agent_lunch_logout TIMESTAMP,
  agent_lunch_duration STRING,
  agent_city STRING,
  agent_state STRING,
  agent_country STRING,
  agent_zip_code STRING,
  data_loaded_at TIMESTAMP
)
RETURNS STRING
RETURN SHA2(
  CONCAT(
    COALESCE(unix_id,""),
    COALESCE(agent_name,""),
    COALESCE(CAST(log_date AS STRING),""),
    COALESCE(CAST(agent_first_login AS STRING),""),
    COALESCE(CAST(agent_first_logout AS STRING),""),
    COALESCE(total_login_time_hrs,""),
    COALESCE(CAST(agent_lunch_login AS STRING),""),
    COALESCE(CAST(agent_lunch_logout AS STRING),""),
    COALESCE(agent_lunch_duration,""),
    COALESCE(agent_city,""),
    COALESCE(agent_state,""),
    COALESCE(agent_country,""),
    COALESCE(agent_zip_code,""),
    COALESCE(CAST(data_loaded_at AS STRING),"")
  ),
  256
);

/*-----------------------------------------------------------------------------
SECTION: Main CTE - Agent Log Calculated Field
-----------------------------------------------------------------------------*/
WITH
-- CTE: Most recent agent profile per email_address
latest_agent_profile AS (
  SELECT
    ap.email_address,
    ap.internal_user_key,
    ap.full_name,
    ap.last_modified_timestamp,
    ROW_NUMBER() OVER (
      PARTITION BY ap.email_address
      ORDER BY ap.last_modified_timestamp DESC
    ) AS rn
  FROM purgo_playground.agent_profile_data ap
),
-- CTE: Join session_tracking_data with user_presence_tracker and agent_profile_data
agent_log_base AS (
  SELECT
    -- unix_id: internal_user_key from agent_profile_data
    lap.internal_user_key AS unix_id,
    -- agent_name: full_name from agent_profile_data
    lap.full_name AS agent_name,
    -- log_date: date part of session_start_time
    DATE(std.session_start_time) AS log_date,
    -- agent_first_login: min(session_start_time) per agent per log_date
    MIN(std.session_start_time) OVER (
      PARTITION BY lap.internal_user_key, DATE(std.session_start_time)
    ) AS agent_first_login,
    -- agent_first_logout: max(session_end_time) per agent per log_date
    MAX(std.session_end_time) OVER (
      PARTITION BY lap.internal_user_key, DATE(std.session_start_time)
    ) AS agent_first_logout,
    -- total_login_time_hrs: (agent_first_logout - agent_first_login) in hours, 2 decimal places
    LPAD(
      CAST(
        ROUND(
          (
            UNIX_TIMESTAMP(
              MAX(std.session_end_time) OVER (
                PARTITION BY lap.internal_user_key, DATE(std.session_start_time)
              )
            ) -
            UNIX_TIMESTAMP(
              MIN(std.session_start_time) OVER (
                PARTITION BY lap.internal_user_key, DATE(std.session_start_time)
              )
            )
          ) / 3600.0
        ,2) AS STRING
      ),
      4, "0"
    ) AS total_login_time_hrs,
    -- agent_lunch_login, agent_lunch_logout, agent_lunch_duration: NULL (not mapped)
    CAST(NULL AS TIMESTAMP) AS agent_lunch_login,
    CAST(NULL AS TIMESTAMP) AS agent_lunch_logout,
    CAST(NULL AS STRING) AS agent_lunch_duration,
    -- Hardcoded fields
    "New York" AS agent_city,
    "NY" AS agent_state,
    "USA" AS agent_country,
    "10001" AS agent_zip_code,
    -- data_loaded_at: current timestamp
    CURRENT_TIMESTAMP() AS data_loaded_at,
    -- For error logging
    std.person_identifier,
    upt.email_address,
    lap.last_modified_timestamp,
    std.session_start_time,
    std.session_end_time
  FROM purgo_playground.session_tracking_data std
  LEFT JOIN purgo_playground.user_presence_tracker upt
    ON std.person_identifier = upt.person_identifier
  LEFT JOIN latest_agent_profile lap
    ON upt.email_address = lap.email_address AND lap.rn = 1
)
-- Final SELECT: Only valid records, calculated agent_key
SELECT
  unix_id,
  agent_name,
  log_date,
  agent_first_login,
  agent_first_logout,
  total_login_time_hrs,
  agent_lunch_login,
  agent_lunch_logout,
  agent_lunch_duration,
  agent_city,
  agent_state,
  agent_country,
  agent_zip_code,
  data_loaded_at,
  purgo_playground.agent_log_surrogate_key(
    unix_id,
    agent_name,
    log_date,
    agent_first_login,
    agent_first_logout,
    total_login_time_hrs,
    agent_lunch_login,
    agent_lunch_logout,
    agent_lunch_duration,
    agent_city,
    agent_state,
    agent_country,
    agent_zip_code,
    data_loaded_at
  ) AS agent_key
FROM agent_log_base
WHERE
  unix_id IS NOT NULL
  AND agent_name IS NOT NULL
  AND log_date IS NOT NULL
  AND agent_first_login IS NOT NULL
  AND agent_first_logout IS NOT NULL
  AND total_login_time_hrs IS NOT NULL
  -- Optional filters: uncomment as needed
  -- AND log_date >= DATE('2025-01-02') AND log_date <= DATE('2025-01-06')
  -- AND unix_id = "IU011"
;

/*-----------------------------------------------------------------------------
SECTION: Error Logging - Insert error records for missing/null data
-----------------------------------------------------------------------------*/
-- Error: Missing person_identifier
INSERT INTO purgo_playground.agent_log_error_log
SELECT
  "Missing person_identifier" AS error_type,
  "person_identifier is NULL in session_tracking_data" AS error_message,
  CURRENT_TIMESTAMP() AS error_time
FROM agent_log_base
WHERE person_identifier IS NULL;

-- Error: Missing email_address
INSERT INTO purgo_playground.agent_log_error_log
SELECT
  "Missing email_address" AS error_type,
  CONCAT("No email_address found for person_identifier ", person_identifier) AS error_message,
  CURRENT_TIMESTAMP() AS error_time
FROM agent_log_base
WHERE email_address IS NULL AND person_identifier IS NOT NULL;

-- Error: Missing agent_profile_data
INSERT INTO purgo_playground.agent_log_error_log
SELECT
  "Missing agent_profile_data" AS error_type,
  CONCAT("No agent_profile_data found for email_address ", email_address) AS error_message,
  CURRENT_TIMESTAMP() AS error_time
FROM agent_log_base
WHERE unix_id IS NULL AND email_address IS NOT NULL;

-- Error: NULL session_start_time or session_end_time
INSERT INTO purgo_playground.agent_log_error_log
SELECT
  "Missing session times" AS error_type,
  CONCAT("session_start_time or session_end_time is NULL for person_identifier ", person_identifier) AS error_message,
  CURRENT_TIMESTAMP() AS error_time
FROM agent_log_base
WHERE (session_start_time IS NULL OR session_end_time IS NULL) AND person_identifier IS NOT NULL;

-- Error: Data type mismatch (agent_name NULL)
INSERT INTO purgo_playground.agent_log_error_log
SELECT
  "Data type mismatch" AS error_type,
  CONCAT("Field agent_name is NULL for unix_id ", unix_id) AS error_message,
  CURRENT_TIMESTAMP() AS error_time
FROM agent_log_base
WHERE agent_name IS NULL AND unix_id IS NOT NULL;

-- Error: Hashing error (agent_key could not be generated)
INSERT INTO purgo_playground.agent_log_error_log
SELECT
  "Hashing error" AS error_type,
  "agent_key could not be generated due to null fields" AS error_message,
  CURRENT_TIMESTAMP() AS error_time
FROM agent_log_base
WHERE unix_id IS NOT NULL AND (
  agent_name IS NULL OR log_date IS NULL OR agent_first_login IS NULL OR agent_first_logout IS NULL OR total_login_time_hrs IS NULL
);

-- Error: All fields NULL
INSERT INTO purgo_playground.agent_log_error_log
SELECT
  "All fields NULL" AS error_type,
  "All agent_log fields are NULL in source data" AS error_message,
  CURRENT_TIMESTAMP() AS error_time
FROM agent_log_base
WHERE
  unix_id IS NULL AND agent_name IS NULL AND log_date IS NULL AND agent_first_login IS NULL AND agent_first_logout IS NULL AND total_login_time_hrs IS NULL
;

/*-----------------------------------------------------------------------------
END OF AGENT LOG CALCULATED FIELD QUERY
-----------------------------------------------------------------------------*/

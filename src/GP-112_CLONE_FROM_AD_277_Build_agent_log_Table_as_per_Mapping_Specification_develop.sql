USE CATALOG purgo_databricks;

/* 
===============================================================================
Databricks SQL Implementation: Agent Log Calculated Field Query
===============================================================================
Unity Catalog: purgo_databricks
Schema: purgo_playground
Target Table: agent_log (for display only, not insert/update)
Error Logging Table: agent_log_error_log

Business Logic:
- Joins session_tracking_data, user_presence_tracker, agent_profile_data
- Most recent agent_name/unix_id per email_address (by last_modified_timestamp)
- Multiple sessions per agent per day: first_login = min(session_start_time), first_logout = max(session_end_time)
- total_login_time_hrs = (first_logout - first_login) in hours, formatted as string with 2 decimals
- Hardcoded city/state/country/zip
- agent_key = SHA256(concat(all output fields))
- Error logging for missing/nulls as per specification

===============================================================================
*/

/*-----------------------------------------------------------------------------
SECTION: Output Column Documentation
-----------------------------------------------------------------------------*/
COMMENT ON COLUMN purgo_playground.agent_log.unix_id IS 'Agent internal user key, derived from agent_profile_data.internal_user_key. Not null if agent exists.';
COMMENT ON COLUMN purgo_playground.agent_log.agent_name IS 'Agent full name, most recent from agent_profile_data.full_name by last_modified_timestamp. Not null if agent exists.';
COMMENT ON COLUMN purgo_playground.agent_log.log_date IS 'Date of agent session (YYYY-MM-DD), derived from session_start_time.';
COMMENT ON COLUMN purgo_playground.agent_log.agent_first_login IS 'Earliest session_start_time for agent on log_date.';
COMMENT ON COLUMN purgo_playground.agent_log.agent_first_logout IS 'Latest session_end_time for agent on log_date.';
COMMENT ON COLUMN purgo_playground.agent_log.total_login_time_hrs IS 'Difference between agent_first_logout and agent_first_login in hours, formatted as string with 2 decimals.';
COMMENT ON COLUMN purgo_playground.agent_log.agent_lunch_login IS 'Lunch login timestamp, if available. Nullable.';
COMMENT ON COLUMN purgo_playground.agent_log.agent_lunch_logout IS 'Lunch logout timestamp, if available. Nullable.';
COMMENT ON COLUMN purgo_playground.agent_log.agent_lunch_duration IS 'Lunch duration in hours, formatted as string. Nullable.';
COMMENT ON COLUMN purgo_playground.agent_log.agent_city IS 'Hardcoded value: "New York".';
COMMENT ON COLUMN purgo_playground.agent_log.agent_state IS 'Hardcoded value: "NY".';
COMMENT ON COLUMN purgo_playground.agent_log.agent_country IS 'Hardcoded value: "USA".';
COMMENT ON COLUMN purgo_playground.agent_log.agent_zip_code IS 'Hardcoded value: "10001".';
COMMENT ON COLUMN purgo_playground.agent_log.data_loaded_at IS 'Timestamp when data was loaded. CURRENT_TIMESTAMP.';
COMMENT ON COLUMN purgo_playground.agent_log.agent_key IS 'SHA256 hash of all output fields concatenated. 64-character hexadecimal string.';

/*-----------------------------------------------------------------------------
SECTION: Calculated Field Query (CTE-based, display only)
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
-- CTE: Join session_tracking_data with user_presence_tracker and latest_agent_profile
session_agent_data AS (
  SELECT
    std.person_identifier,
    upt.email_address,
    lap.internal_user_key AS unix_id,
    lap.full_name AS agent_name,
    std.session_start_time,
    std.session_end_time
  FROM purgo_playground.session_tracking_data std
  LEFT JOIN purgo_playground.user_presence_tracker upt
    ON std.person_identifier = upt.person_identifier
  LEFT JOIN latest_agent_profile lap
    ON upt.email_address = lap.email_address AND lap.rn = 1
),
-- CTE: Aggregate sessions per agent per day
agent_daily_sessions AS (
  SELECT
    sad.unix_id,
    sad.agent_name,
    DATE(sad.session_start_time) AS log_date,
    MIN(sad.session_start_time) AS agent_first_login,
    MAX(sad.session_end_time) AS agent_first_logout,
    -- Lunch fields: Not available in source, set as NULL
    CAST(NULL AS TIMESTAMP) AS agent_lunch_login,
    CAST(NULL AS TIMESTAMP) AS agent_lunch_logout,
    CAST(NULL AS STRING) AS agent_lunch_duration,
    'New York' AS agent_city,
    'NY' AS agent_state,
    'USA' AS agent_country,
    '10001' AS agent_zip_code,
    CURRENT_TIMESTAMP() AS data_loaded_at
  FROM session_agent_data sad
  WHERE sad.person_identifier IS NOT NULL
    AND sad.session_start_time IS NOT NULL
    AND sad.session_end_time IS NOT NULL
    AND sad.unix_id IS NOT NULL
    AND sad.agent_name IS NOT NULL
  GROUP BY
    sad.unix_id,
    sad.agent_name,
    DATE(sad.session_start_time)
),
-- CTE: Calculate total_login_time_hrs and agent_key
agent_log_calculated AS (
  SELECT
    unix_id,
    agent_name,
    log_date,
    agent_first_login,
    agent_first_logout,
    -- Calculate total_login_time_hrs: difference in hours, formatted as string with 2 decimals
    LPAD(
      CAST(
        ROUND(
          (UNIX_TIMESTAMP(agent_first_logout) - UNIX_TIMESTAMP(agent_first_login)) / 3600.0
        ,2) AS STRING
      )
    ,4,'0') AS total_login_time_hrs,
    agent_lunch_login,
    agent_lunch_logout,
    agent_lunch_duration,
    agent_city,
    agent_state,
    agent_country,
    agent_zip_code,
    data_loaded_at,
    -- agent_key: SHA256 of all output fields concatenated as string
    SHA2(
      CONCAT(
        COALESCE(unix_id,''),
        COALESCE(agent_name,''),
        COALESCE(CAST(log_date AS STRING),''),
        COALESCE(CAST(agent_first_login AS STRING),''),
        COALESCE(CAST(agent_first_logout AS STRING),''),
        COALESCE(total_login_time_hrs,''),
        COALESCE(CAST(agent_lunch_login AS STRING),''),
        COALESCE(CAST(agent_lunch_logout AS STRING),''),
        COALESCE(agent_lunch_duration,''),
        COALESCE(agent_city,''),
        COALESCE(agent_state,''),
        COALESCE(agent_country,''),
        COALESCE(agent_zip_code,''),
        COALESCE(CAST(data_loaded_at AS STRING),'')
      ),256
    ) AS agent_key
  FROM agent_daily_sessions
)

-- Final SELECT: Display calculated fields for agent_log
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
  agent_key
FROM agent_log_calculated
-- Example filters: Uncomment as needed
-- WHERE log_date >= DATE('2025-01-02') AND log_date <= DATE('2025-01-06')
--   AND unix_id = 'IU011'
;

/*-----------------------------------------------------------------------------
SECTION: Error Logging for Missing/NULL Data
-----------------------------------------------------------------------------*/
-- Log error: Missing person_identifier in session_tracking_data
INSERT INTO purgo_playground.agent_log_error_log
SELECT
  'Missing person_identifier' AS error_type,
  'person_identifier is NULL in session_tracking_data' AS error_message,
  CURRENT_TIMESTAMP() AS error_time
FROM purgo_playground.session_tracking_data
WHERE person_identifier IS NULL
;

-- Log error: Missing email_address in user_presence_tracker
INSERT INTO purgo_playground.agent_log_error_log
SELECT
  'Missing email_address' AS error_type,
  CONCAT('No email_address found for person_identifier ', std.person_identifier) AS error_message,
  CURRENT_TIMESTAMP() AS error_time
FROM purgo_playground.session_tracking_data std
LEFT JOIN purgo_playground.user_presence_tracker upt
  ON std.person_identifier = upt.person_identifier
WHERE std.person_identifier IS NOT NULL
  AND upt.email_address IS NULL
;

-- Log error: Missing agent_profile_data for email_address
INSERT INTO purgo_playground.agent_log_error_log
SELECT
  'Missing agent_profile_data' AS error_type,
  CONCAT('No agent_profile_data found for email_address ', upt.email_address) AS error_message,
  CURRENT_TIMESTAMP() AS error_time
FROM purgo_playground.session_tracking_data std
LEFT JOIN purgo_playground.user_presence_tracker upt
  ON std.person_identifier = upt.person_identifier
LEFT JOIN purgo_playground.agent_profile_data ap
  ON upt.email_address = ap.email_address
WHERE std.person_identifier IS NOT NULL
  AND upt.email_address IS NOT NULL
  AND ap.email_address IS NULL
;

-- Log error: NULL session_start_time or session_end_time
INSERT INTO purgo_playground.agent_log_error_log
SELECT
  'Missing session times' AS error_type,
  CONCAT('session_start_time or session_end_time is NULL for person_identifier ', std.person_identifier) AS error_message,
  CURRENT_TIMESTAMP() AS error_time
FROM purgo_playground.session_tracking_data std
WHERE std.person_identifier IS NOT NULL
  AND (std.session_start_time IS NULL OR std.session_end_time IS NULL)
;

/*-----------------------------------------------------------------------------
END OF IMPLEMENTATION
-----------------------------------------------------------------------------*/

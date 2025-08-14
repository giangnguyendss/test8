/*==============================================================================
Agent Log Calculated Field Extraction and Error Logging
==============================================================================
- Catalog: purgo_databricks
- Schema: purgo_playground
- Target Table: agent_log
- Error Table: agent_log_error_log

This script:
  - Extracts calculated fields for agent_log as per mapping specification
  - Handles all error scenarios and logs to agent_log_error_log
  - Applies business rules for most recent agent_profile_data, hardcoded location, surrogate key
  - Ensures schema/data type consistency and robust NULL handling
  - Uses CTEs for modular logic and validation
==============================================================================*/

USE CATALOG purgo_databricks;

/*-----------------------------------------------------------------------------
SECTION: Calculated Field Extraction for agent_log
-----------------------------------------------------------------------------*/

-- CTE: Join session_tracking_data with user_presence_tracker to get email_address
CREATE OR REPLACE TEMP VIEW st_upt AS
SELECT
  st.person_identifier,
  st.session_start_time,
  st.session_end_time,
  upt.email_address
FROM purgo_playground.session_tracking_data st
LEFT JOIN purgo_playground.user_presence_tracker upt
  ON st.person_identifier = upt.person_identifier
;

-- CTE: For each email_address, select most recent agent_profile_data (by last_modified_timestamp)
CREATE OR REPLACE TEMP VIEW most_recent_profile AS
SELECT
  email_address,
  internal_user_key,
  full_name,
  last_modified_timestamp
FROM (
  SELECT
    email_address,
    internal_user_key,
    full_name,
    last_modified_timestamp,
    ROW_NUMBER() OVER (PARTITION BY email_address ORDER BY last_modified_timestamp DESC) AS rn,
    COUNT(*) OVER (PARTITION BY email_address, last_modified_timestamp) AS ts_dupe_count
  FROM purgo_playground.agent_profile_data
) t
WHERE rn = 1
;

-- CTE: Error - Missing email_address in user_presence_tracker
CREATE OR REPLACE TEMP VIEW missing_email AS
SELECT
  st.person_identifier,
  st.session_start_time,
  st.session_end_time
FROM purgo_playground.session_tracking_data st
LEFT JOIN purgo_playground.user_presence_tracker upt
  ON st.person_identifier = upt.person_identifier
WHERE upt.email_address IS NULL
;

-- CTE: Error - Missing agent_profile_data for email_address
CREATE OR REPLACE TEMP VIEW missing_profile AS
SELECT
  st_upt.person_identifier,
  st_upt.email_address
FROM st_upt
LEFT JOIN most_recent_profile mrp
  ON st_upt.email_address = mrp.email_address
WHERE st_upt.email_address IS NOT NULL
  AND mrp.internal_user_key IS NULL
;

-- CTE: Error - Missing session_end_time in session_tracking_data
CREATE OR REPLACE TEMP VIEW missing_end_time AS
SELECT
  st_upt.person_identifier
FROM st_upt
WHERE st_upt.session_end_time IS NULL
  AND st_upt.email_address IS NOT NULL
;

-- CTE: Error - NonDeterministicMostRecent (multiple agent_profile_data records with identical last_modified_timestamp)
CREATE OR REPLACE TEMP VIEW nondeterministic_most_recent AS
SELECT
  apd.email_address
FROM purgo_playground.agent_profile_data apd
JOIN (
  SELECT
    email_address,
    last_modified_timestamp,
    COUNT(*) AS cnt
  FROM purgo_playground.agent_profile_data
  GROUP BY email_address, last_modified_timestamp
  HAVING COUNT(*) > 1
) dupe
  ON apd.email_address = dupe.email_address
  AND apd.last_modified_timestamp = dupe.last_modified_timestamp
;

-- CTE: Final join for valid agent_log records
CREATE OR REPLACE TEMP VIEW agent_log_valid AS
SELECT
  mrp.internal_user_key AS unix_id,
  mrp.full_name AS agent_name,
  DATE(st_upt.session_start_time) AS log_date,
  st_upt.session_start_time AS agent_first_login,
  st_upt.session_end_time AS agent_first_logout,
  CAST(
    ROUND(
      (UNIX_TIMESTAMP(st_upt.session_end_time) - UNIX_TIMESTAMP(st_upt.session_start_time)) / 3600.0, 1
    ) AS STRING
  ) AS total_login_time_hrs,
  NULL AS agent_lunch_login,
  NULL AS agent_lunch_logout,
  NULL AS agent_lunch_duration,
  "New York" AS agent_city,
  "NY" AS agent_state,
  "USA" AS agent_country,
  "10001" AS agent_zip_code,
  current_timestamp() AS data_loaded_at,
  sha2(
    concat_ws("|",
      mrp.internal_user_key,
      mrp.full_name,
      CAST(DATE(st_upt.session_start_time) AS STRING),
      CAST(st_upt.session_start_time AS STRING),
      CAST(st_upt.session_end_time AS STRING),
      CAST(
        ROUND(
          (UNIX_TIMESTAMP(st_upt.session_end_time) - UNIX_TIMESTAMP(st_upt.session_start_time)) / 3600.0, 1
        ) AS STRING
      ),
      "New York",
      "NY",
      "USA",
      "10001",
      CAST(current_timestamp() AS STRING)
    ), 256
  ) AS agent_key
FROM st_upt
LEFT JOIN most_recent_profile mrp
  ON st_upt.email_address = mrp.email_address
WHERE st_upt.email_address IS NOT NULL
  AND mrp.internal_user_key IS NOT NULL
  AND st_upt.session_start_time IS NOT NULL
  AND st_upt.session_end_time IS NOT NULL
  AND mrp.email_address NOT IN (SELECT email_address FROM nondeterministic_most_recent)
;

-- Insert valid records into agent_log
INSERT INTO purgo_playground.agent_log
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
FROM agent_log_valid
;

/*-----------------------------------------------------------------------------
SECTION: Error Logging for agent_log_error_log
-----------------------------------------------------------------------------*/

-- Error: Missing email_address in user_presence_tracker
INSERT INTO purgo_playground.agent_log_error_log
SELECT
  "MissingEmailAddress" AS error_type,
  concat("No email_address found for person_identifier ", person_identifier, " in user_presence_tracker") AS error_message,
  current_timestamp() AS error_time
FROM missing_email
;

-- Error: Missing agent_profile_data for email_address
INSERT INTO purgo_playground.agent_log_error_log
SELECT
  "MissingAgentProfileData" AS error_type,
  concat("No agent_profile_data found for email_address ", email_address) AS error_message,
  current_timestamp() AS error_time
FROM missing_profile
;

-- Error: Missing session_end_time in session_tracking_data
INSERT INTO purgo_playground.agent_log_error_log
SELECT
  "MissingSessionEndTime" AS error_type,
  concat("No session_end_time found for person_identifier ", person_identifier) AS error_message,
  current_timestamp() AS error_time
FROM missing_end_time
;

-- Error: NonDeterministicMostRecent (multiple agent_profile_data records with identical last_modified_timestamp)
INSERT INTO purgo_playground.agent_log_error_log
SELECT
  "NonDeterministicMostRecent" AS error_type,
  concat("Multiple agent_profile_data records for email_address ", email_address, " have identical last_modified_timestamp") AS error_message,
  current_timestamp() AS error_time
FROM nondeterministic_most_recent
;

/*-----------------------------------------------------------------------------
SECTION: Validation Query - Display Calculated Fields from agent_log
-----------------------------------------------------------------------------*/

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
FROM purgo_playground.agent_log
ORDER BY log_date, unix_id
;

/*-----------------------------------------------------------------------------
END OF SCRIPT
-----------------------------------------------------------------------------*/


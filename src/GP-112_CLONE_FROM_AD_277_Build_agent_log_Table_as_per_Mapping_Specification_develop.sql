USE CATALOG purgo_databricks;

/*==============================================================================
  Databricks SQL Implementation: Agent Log Calculated Field Extraction
  ------------------------------------------------------------------------------
  Unity Catalog: purgo_databricks
  Schema: purgo_playground
  Target Table: agent_log
  Source Tables: session_tracking_data, user_presence_tracker, agent_profile_data
  ------------------------------------------------------------------------------
  Logic:
    - For each agent session (person_identifier), join to user_presence_tracker for email_address.
    - Join to agent_profile_data for most recent internal_user_key and full_name by last_modified_timestamp.
    - Aggregate per agent per log_date:
        * agent_first_login: earliest session_start_time
        * agent_first_logout: latest session_end_time
        * total_login_time_hrs: (logout - login) in hours, as string
    - Hardcode agent_city, agent_state, agent_country, agent_zip_code.
    - data_loaded_at: current timestamp
    - agent_key: MD5 hash of all fields (as string)
    - agent_lunch_login, agent_lunch_logout, agent_lunch_duration: NULL (no source mapping)
    - NULL handling: propagate NULLs for missing joins or session times
  ------------------------------------------------------------------------------
  Performance:
    - Window functions for aggregation
    - CTE for modularity and validation
    - All string fields use STRING type
    - All date fields use DATE or TIMESTAMP type
    - Explicit column aliases for all output columns
==============================================================================*/

/*-----------------------------------------------------------------------------
  SECTION: UDF Definitions (if needed)
-----------------------------------------------------------------------------*/
/* No custom UDFs required for this implementation */

/*-----------------------------------------------------------------------------
  SECTION: Main Query - Agent Log Calculated Field Extraction
-----------------------------------------------------------------------------*/
WITH session_enriched AS (
  /* Join session_tracking_data to user_presence_tracker for email_address */
  SELECT
    std.person_identifier AS person_identifier,
    std.session_start_time AS session_start_time,
    std.session_end_time AS session_end_time,
    CAST(std.session_start_time AS DATE) AS log_date,
    upt.email_address AS email_address
  FROM purgo_playground.session_tracking_data AS std
  LEFT JOIN purgo_playground.user_presence_tracker AS upt
    ON std.person_identifier = upt.person_identifier
),
profile_latest AS (
  /* For each email_address, get most recent agent_profile_data */
  SELECT
    apd.email_address AS email_address,
    apd.internal_user_key AS unix_id,
    apd.full_name AS agent_name,
    apd.last_modified_timestamp,
    ROW_NUMBER() OVER (
      PARTITION BY apd.email_address
      ORDER BY apd.last_modified_timestamp DESC
    ) AS rn
  FROM purgo_playground.agent_profile_data AS apd
),
session_profile AS (
  /* Join session_enriched to profile_latest for agent info */
  SELECT
    se.person_identifier,
    se.session_start_time,
    se.session_end_time,
    se.log_date,
    se.email_address,
    pl.unix_id,
    pl.agent_name
  FROM session_enriched AS se
  LEFT JOIN profile_latest AS pl
    ON se.email_address = pl.email_address AND pl.rn = 1
),
agent_log_agg AS (
  /* Aggregate per agent per log_date for login/logout and duration */
  SELECT
    sp.unix_id AS unix_id,
    sp.agent_name AS agent_name,
    sp.log_date AS log_date,
    MIN(sp.session_start_time) AS agent_first_login,
    MAX(sp.session_end_time) AS agent_first_logout,
    /* Calculate total_login_time_hrs as string, NULL if either is NULL */
    CASE
      WHEN MIN(sp.session_start_time) IS NOT NULL AND MAX(sp.session_end_time) IS NOT NULL
        THEN CAST(
          ROUND(
            (UNIX_TIMESTAMP(MAX(sp.session_end_time)) - UNIX_TIMESTAMP(MIN(sp.session_start_time))) / 3600.0, 2
          ) AS STRING
        )
      ELSE NULL
    END AS total_login_time_hrs,
    /* agent_lunch_login, agent_lunch_logout, agent_lunch_duration: NULL (no mapping) */
    CAST(NULL AS TIMESTAMP) AS agent_lunch_login,
    CAST(NULL AS TIMESTAMP) AS agent_lunch_logout,
    CAST(NULL AS STRING) AS agent_lunch_duration,
    /* Hardcoded location fields */
    "New York" AS agent_city,
    "NY" AS agent_state,
    "USA" AS agent_country,
    "10001" AS agent_zip_code,
    /* data_loaded_at: current timestamp */
    CURRENT_TIMESTAMP() AS data_loaded_at
  FROM session_profile AS sp
  GROUP BY
    sp.unix_id,
    sp.agent_name,
    sp.log_date
),
agent_log_final AS (
  /* Add agent_key as MD5 hash of all fields (as string) */
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
    md5(
      concat_ws(
        '|',
        COALESCE(unix_id, ''),
        COALESCE(agent_name, ''),
        COALESCE(CAST(log_date AS STRING), ''),
        COALESCE(CAST(agent_first_login AS STRING), ''),
        COALESCE(CAST(agent_first_logout AS STRING), ''),
        COALESCE(total_login_time_hrs, ''),
        COALESCE(CAST(agent_lunch_login AS STRING), ''),
        COALESCE(CAST(agent_lunch_logout AS STRING), ''),
        COALESCE(agent_lunch_duration, ''),
        COALESCE(agent_city, ''),
        COALESCE(agent_state, ''),
        COALESCE(agent_country, ''),
        COALESCE(agent_zip_code, ''),
        COALESCE(CAST(data_loaded_at AS STRING), '')
      )
    ) AS agent_key
  FROM agent_log_agg
)

/*-----------------------------------------------------------------------------
  SECTION: Validation Query - Display Calculated Agent Log Fields
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
FROM agent_log_final
ORDER BY log_date, unix_id
;
-- End of implementation

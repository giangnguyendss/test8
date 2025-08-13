USE CATALOG purgo_databricks;

/*
  Enhanced SQL Transformation for purgo_playground.pat_account
  - Implements updated transformation logic for prescriber_id, prescriber_key, patient_sf_id, hash_key, last_modified_date
  - Applies mapping rules from source_target_mapping_specifications.xlsx
  - Handles error logging and ETL event logging
  - Only records where patinet_c contains 'PAT' are eligible for transformation
  - All string fields use STRING type, last_modified_date uses TIMESTAMP
  - All transformation logic is documented in column comments
  - Ensures schema consistency and data quality
  - Catalog: purgo_databricks
  - Schema: purgo_playground
*/

/* ---------- DDL: Create pat_account table with updated schema ---------- */
CREATE TABLE IF NOT EXISTS purgo_playground.pat_account (
  patient_foundation_shipment STRING,
  prescriber_id STRING,
  prescriber_key STRING,
  patient_sf_id STRING,
  service_request_type STRING,
  case_sf_id STRING,
  account_id STRING,
  hash_key STRING,
  last_modified_date TIMESTAMP
);

-- Add column comments for documentation
COMMENT ON COLUMN purgo_playground.pat_account.patient_foundation_shipment IS 'Straight move from source';
COMMENT ON COLUMN purgo_playground.pat_account.prescriber_id IS 'Convert name to uppercase, prefix "DR." only if not present';
COMMENT ON COLUMN purgo_playground.pat_account.prescriber_key IS 'Convert address to uppercase, prefix "DR." only if not present';
COMMENT ON COLUMN purgo_playground.pat_account.patient_sf_id IS 'Set only if patinet_c contains "PAT"; else NULL';
COMMENT ON COLUMN purgo_playground.pat_account.service_request_type IS 'Straight move from source';
COMMENT ON COLUMN purgo_playground.pat_account.case_sf_id IS 'Straight move from source';
COMMENT ON COLUMN purgo_playground.pat_account.account_id IS 'Straight move from source';
COMMENT ON COLUMN purgo_playground.pat_account.hash_key IS 'Concatenation of patinet_c, recordtypeid, id with underscores (_); NULLs replaced by empty string';
COMMENT ON COLUMN purgo_playground.pat_account.last_modified_date IS 'Set to current timestamp for all transformed records';

/* ---------- DDL: Create error log and ETL log tables if not exist ---------- */
CREATE TABLE IF NOT EXISTS purgo_playground.pat_account_error_log (
  hash_key STRING,
  error_col STRING NOT NULL,
  error_message STRING NOT NULL,
  event_time TIMESTAMP
);

CREATE TABLE IF NOT EXISTS purgo_playground.pat_account_etl_log (
  log_ts TIMESTAMP,
  patient_sf_id STRING,
  prescriber_id STRING,
  hash_key STRING,
  last_modified_date TIMESTAMP,
  account_id STRING,
  error_message STRING
);

/* ---------- CTE: Source Data Extraction ---------- */
WITH source_pat_account AS (
  SELECT
    patient_foundation_shipment,
    prescriber_name_c,
    prescriber_name_c_address,
    patinet_c,
    recordtypeid,
    id,
    accountid
  FROM purgo_playground.p_account
),

/* ---------- CTE: Transformation Logic ---------- */
transformed_pat_account AS (
  SELECT
    -- patient_foundation_shipment: straight move if patinet_c contains 'PAT', else NULL
    CASE WHEN patinet_c LIKE '%PAT%' THEN patient_foundation_shipment ELSE NULL END AS patient_foundation_shipment,

    -- prescriber_id: uppercase, prefix 'DR.' only if not present (case-insensitive)
    CASE
      WHEN prescriber_name_c IS NULL THEN NULL
      WHEN UPPER(TRIM(prescriber_name_c)) LIKE 'DR.%' THEN UPPER(TRIM(prescriber_name_c))
      ELSE CONCAT('DR.', UPPER(TRIM(prescriber_name_c)))
    END AS prescriber_id,

    -- prescriber_key: uppercase, prefix 'DR.' only if not present (case-insensitive)
    CASE
      WHEN prescriber_name_c_address IS NULL THEN NULL
      WHEN UPPER(TRIM(prescriber_name_c_address)) LIKE 'DR.%' THEN UPPER(TRIM(prescriber_name_c_address))
      ELSE CONCAT('DR.', UPPER(TRIM(prescriber_name_c_address)))
    END AS prescriber_key,

    -- patient_sf_id: only if patinet_c contains 'PAT', else NULL
    CASE WHEN patinet_c LIKE '%PAT%' THEN patinet_c ELSE NULL END AS patient_sf_id,

    -- service_request_type: straight move
    recordtypeid AS service_request_type,

    -- case_sf_id: straight move
    id AS case_sf_id,

    -- account_id: straight move
    accountid AS account_id,

    -- hash_key: concatenation of patinet_c, recordtypeid, id with underscores; NULLs handled
    CONCAT(
      COALESCE(patinet_c, ''),
      '_',
      COALESCE(recordtypeid, ''),
      '_',
      COALESCE(id, '')
    ) AS hash_key,

    -- last_modified_date: always current timestamp in Databricks format
    CURRENT_TIMESTAMP() AS last_modified_date

  FROM source_pat_account
),

/* ---------- CTE: Error Detection Logic ---------- */
error_pat_account AS (
  SELECT
    -- hash_key: always generated, even if some fields are NULL
    CONCAT(
      COALESCE(patinet_c, ''),
      '_',
      COALESCE(recordtypeid, ''),
      '_',
      COALESCE(id, '')
    ) AS hash_key,

    -- error_col: which column is in error
    CASE
      WHEN patinet_c IS NULL OR patinet_c NOT LIKE '%PAT%' THEN 'patient_sf_id'
      WHEN prescriber_name_c IS NULL THEN 'prescriber_id'
      WHEN prescriber_name_c_address IS NULL THEN 'prescriber_key'
      WHEN recordtypeid IS NULL THEN 'hash_key'
      WHEN id IS NULL THEN 'hash_key'
      ELSE NULL
    END AS error_col,

    -- error_message: detailed error
    CASE
      WHEN patinet_c IS NULL THEN 'patinet_c is NULL for hash_key'
      WHEN patinet_c NOT LIKE '%PAT%' THEN 'patinet_c does not contain ''PAT'''
      WHEN prescriber_name_c IS NULL THEN 'prescriber_name_c is NULL'
      WHEN prescriber_name_c_address IS NULL THEN 'prescriber_name_c_address is NULL'
      WHEN recordtypeid IS NULL THEN 'recordtypeid is NULL for hash_key'
      WHEN id IS NULL THEN 'id is NULL for hash_key'
      ELSE NULL
    END AS error_message,

    -- event_time: current timestamp
    CURRENT_TIMESTAMP() AS event_time

  FROM source_pat_account
  WHERE
    patinet_c IS NULL
    OR patinet_c NOT LIKE '%PAT%'
    OR prescriber_name_c IS NULL
    OR prescriber_name_c_address IS NULL
    OR recordtypeid IS NULL
    OR id IS NULL
),

/* ---------- CTE: ETL Event Logging ---------- */
etl_pat_account AS (
  SELECT
    CURRENT_TIMESTAMP() AS log_ts,
    -- patient_sf_id: only if patinet_c contains 'PAT'
    CASE WHEN patinet_c LIKE '%PAT%' THEN patinet_c ELSE NULL END AS patient_sf_id,
    -- prescriber_id: transformation as above
    CASE
      WHEN prescriber_name_c IS NULL THEN NULL
      WHEN UPPER(TRIM(prescriber_name_c)) LIKE 'DR.%' THEN UPPER(TRIM(prescriber_name_c))
      ELSE CONCAT('DR.', UPPER(TRIM(prescriber_name_c)))
    END AS prescriber_id,
    -- hash_key: always generated
    CONCAT(
      COALESCE(patinet_c, ''),
      '_',
      COALESCE(recordtypeid, ''),
      '_',
      COALESCE(id, '')
    ) AS hash_key,
    -- last_modified_date: current timestamp
    CURRENT_TIMESTAMP() AS last_modified_date,
    -- account_id: straight move
    accountid AS account_id,
    -- error_message: NULL for success, else error
    CASE
      WHEN patinet_c IS NULL THEN 'patinet_c is NULL for hash_key'
      WHEN patinet_c NOT LIKE '%PAT%' THEN 'patinet_c does not contain ''PAT'''
      WHEN prescriber_name_c IS NULL THEN 'prescriber_name_c is NULL'
      WHEN prescriber_name_c_address IS NULL THEN 'prescriber_name_c_address is NULL'
      WHEN recordtypeid IS NULL THEN 'recordtypeid is NULL for hash_key'
      WHEN id IS NULL THEN 'id is NULL for hash_key'
      ELSE NULL
    END AS error_message
  FROM source_pat_account
)

/* ---------- INSERT: Transformed Data into pat_account ---------- */
INSERT INTO purgo_playground.pat_account (
  patient_foundation_shipment,
  prescriber_id,
  prescriber_key,
  patient_sf_id,
  service_request_type,
  case_sf_id,
  account_id,
  hash_key,
  last_modified_date
)
SELECT
  patient_foundation_shipment,
  prescriber_id,
  prescriber_key,
  patient_sf_id,
  service_request_type,
  case_sf_id,
  account_id,
  hash_key,
  last_modified_date
FROM transformed_pat_account
WHERE patient_sf_id IS NOT NULL;

/* ---------- INSERT: Error Records into pat_account_error_log ---------- */
INSERT INTO purgo_playground.pat_account_error_log (
  hash_key,
  error_col,
  error_message,
  event_time
)
SELECT
  hash_key,
  error_col,
  error_message,
  event_time
FROM error_pat_account
WHERE error_col IS NOT NULL;

/* ---------- INSERT: ETL Event Log ---------- */
INSERT INTO purgo_playground.pat_account_etl_log (
  log_ts,
  patient_sf_id,
  prescriber_id,
  hash_key,
  last_modified_date,
  account_id,
  error_message
)
SELECT
  log_ts,
  patient_sf_id,
  prescriber_id,
  hash_key,
  last_modified_date,
  account_id,
  error_message
FROM etl_pat_account;

/* ---------- END OF SCRIPT ---------- */
-- All transformation, error, and ETL logging complete

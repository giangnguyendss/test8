USE CATALOG purgo_databricks;

/*
  ---------------------------------------------------------------------------
  Databricks SQL Transformation Script for pat_account Table
  Catalog: purgo_databricks
  Schema: purgo_playground
  Table: pat_account
  ---------------------------------------------------------------------------
  This script updates the transformation logic for prescriber_id, patient_sf_id,
  hash_key, and last_modified_date columns as per the latest mapping specification.
  It also implements error logging for data quality issues.
  ---------------------------------------------------------------------------
*/

/* ------------------ DDL: Ensure pat_account Table Schema ------------------ */
CREATE TABLE IF NOT EXISTS purgo_playground.pat_account (
  patient_foundation_shipment STRING COMMENT 'Straight move from source',
  prescriber_id STRING COMMENT 'Convert name to uppercase, then prefix DR. only if not present (case-insensitive)',
  prescriber_key STRING COMMENT 'Convert to uppercase, prefix DR. only if not present (case-insensitive)',
  patient_sf_id STRING COMMENT 'Set only if patinet_c contains PAT (case-sensitive); else NULL',
  service_request_type STRING COMMENT 'Straight move from source',
  case_sf_id STRING COMMENT 'Straight move from source',
  account_id STRING COMMENT 'Straight move from source',
  hash_key STRING COMMENT 'Concatenation of patinet_c, recordtypeid, id with underscore (_)',
  last_modified_date TIMESTAMP COMMENT 'Current ETL execution timestamp in ISO 8601 format'
);

/* ------------------ COMMENT ON COLUMN for Documentation ------------------ */
COMMENT ON COLUMN purgo_playground.pat_account.prescriber_id IS
  'Convert name to uppercase, then prefix DR. only if not present (case-insensitive)';
COMMENT ON COLUMN purgo_playground.pat_account.prescriber_key IS
  'Convert to uppercase, prefix DR. only if not present (case-insensitive)';
COMMENT ON COLUMN purgo_playground.pat_account.patient_sf_id IS
  'Set only if patinet_c contains PAT (case-sensitive); else NULL';
COMMENT ON COLUMN purgo_playground.pat_account.hash_key IS
  'Concatenation of patinet_c, recordtypeid, id with underscore (_)';
COMMENT ON COLUMN purgo_playground.pat_account.last_modified_date IS
  'Current ETL execution timestamp in ISO 8601 format';

/* ------------------ CTE: Source Data with Transformation & Error Flags ------------------ */
WITH transformed_source AS (
  SELECT
    -- patient_foundation_shipment: only if patinet_c contains 'PAT'
    CASE WHEN patinet_c LIKE '%PAT%' THEN patient_foundation_shipment ELSE NULL END AS patient_foundation_shipment,
    -- prescriber_id: uppercase, prefix DR. if not present (case-insensitive)
    CASE
      WHEN prescriber_name_c IS NULL THEN NULL
      WHEN UPPER(prescriber_name_c) LIKE 'DR.%' THEN UPPER(prescriber_name_c)
      ELSE CONCAT('DR.', UPPER(prescriber_name_c))
    END AS prescriber_id,
    -- prescriber_key: uppercase, prefix DR. if not present (case-insensitive)
    CASE
      WHEN prescriber_name_c_address IS NULL THEN NULL
      WHEN UPPER(prescriber_name_c_address) LIKE 'DR.%' THEN UPPER(prescriber_name_c_address)
      ELSE CONCAT('DR.', UPPER(prescriber_name_c_address))
    END AS prescriber_key,
    -- patient_sf_id: only if patinet_c contains 'PAT', else NULL
    CASE WHEN patinet_c LIKE '%PAT%' THEN patinet_c ELSE NULL END AS patient_sf_id,
    -- service_request_type: straight move
    recordtypeid AS service_request_type,
    -- case_sf_id: straight move
    id AS case_sf_id,
    -- account_id: straight move
    accountid AS account_id,
    -- hash_key: concatenate patinet_c, recordtypeid, id with underscore
    CASE
      WHEN patinet_c IS NULL AND recordtypeid IS NULL AND id IS NULL THEN NULL
      ELSE CONCAT_WS('_', patinet_c, recordtypeid, id)
    END AS hash_key,
    -- last_modified_date: current ETL execution timestamp
    CURRENT_TIMESTAMP() AS last_modified_date,
    -- Error flags for logging
    prescriber_name_c,
    prescriber_name_c_address,
    patinet_c,
    recordtypeid,
    id
  FROM purgo_playground.p_account
)
-- End of CTE

/* ------------------ INSERT: Transformed Data into pat_account ------------------ */
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
FROM transformed_source
WHERE patient_sf_id IS NOT NULL; -- Only insert records where patient_sf_id is set

/* ------------------ ERROR LOGGING: Data Quality Checks ------------------ */
-- Log error if prescriber_name_c is NULL
INSERT INTO purgo_playground.pat_account_error_log (hash_key, error_col, error_message, event_time)
SELECT
  CASE
    WHEN patinet_c IS NULL AND recordtypeid IS NULL AND id IS NULL THEN NULL
    ELSE CONCAT_WS('_', patinet_c, recordtypeid, id)
  END AS hash_key,
  'prescriber_id' AS error_col,
  'prescriber_name_c is NULL' AS error_message,
  CURRENT_TIMESTAMP() AS event_time
FROM transformed_source
WHERE prescriber_name_c IS NULL;

-- Log error if prescriber_name_c_address is NULL
INSERT INTO purgo_playground.pat_account_error_log (hash_key, error_col, error_message, event_time)
SELECT
  CASE
    WHEN patinet_c IS NULL AND recordtypeid IS NULL AND id IS NULL THEN NULL
    ELSE CONCAT_WS('_', patinet_c, recordtypeid, id)
  END AS hash_key,
  'prescriber_key' AS error_col,
  'prescriber_name_c_address is NULL' AS error_message,
  CURRENT_TIMESTAMP() AS event_time
FROM transformed_source
WHERE prescriber_name_c_address IS NULL;

-- Log error if patinet_c is NULL
INSERT INTO purgo_playground.pat_account_error_log (hash_key, error_col, error_message, event_time)
SELECT
  CASE
    WHEN patinet_c IS NULL AND recordtypeid IS NULL AND id IS NULL THEN NULL
    ELSE CONCAT_WS('_', patinet_c, recordtypeid, id)
  END AS hash_key,
  'patient_sf_id' AS error_col,
  'patinet_c is NULL' AS error_message,
  CURRENT_TIMESTAMP() AS event_time
FROM transformed_source
WHERE patinet_c IS NULL;

-- Log error if recordtypeid is NULL for hash_key
INSERT INTO purgo_playground.pat_account_error_log (hash_key, error_col, error_message, event_time)
SELECT
  CASE
    WHEN patinet_c IS NULL AND recordtypeid IS NULL AND id IS NULL THEN NULL
    ELSE CONCAT_WS('_', patinet_c, recordtypeid, id)
  END AS hash_key,
  'hash_key' AS error_col,
  'recordtypeid is NULL' AS error_message,
  CURRENT_TIMESTAMP() AS event_time
FROM transformed_source
WHERE recordtypeid IS NULL AND (patinet_c IS NOT NULL OR id IS NOT NULL);

-- Log error if id is NULL for hash_key
INSERT INTO purgo_playground.pat_account_error_log (hash_key, error_col, error_message, event_time)
SELECT
  CASE
    WHEN patinet_c IS NULL AND recordtypeid IS NULL AND id IS NULL THEN NULL
    ELSE CONCAT_WS('_', patinet_c, recordtypeid, id)
  END AS hash_key,
  'hash_key' AS error_col,
  'id is NULL' AS error_message,
  CURRENT_TIMESTAMP() AS event_time
FROM transformed_source
WHERE id IS NULL AND (patinet_c IS NOT NULL OR recordtypeid IS NOT NULL);

-- Log error if all hash_key components are NULL
INSERT INTO purgo_playground.pat_account_error_log (hash_key, error_col, error_message, event_time)
SELECT
  NULL AS hash_key,
  'hash_key' AS error_col,
  'All hash_key components are NULL' AS error_message,
  CURRENT_TIMESTAMP() AS event_time
FROM transformed_source
WHERE patinet_c IS NULL AND recordtypeid IS NULL AND id IS NULL;

/* ------------------ END OF SCRIPT ------------------ */

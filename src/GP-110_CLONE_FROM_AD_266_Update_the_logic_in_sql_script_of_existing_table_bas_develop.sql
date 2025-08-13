USE CATALOG purgo_databricks;

/*
  Enhanced SQL Transformation Logic for purgo_playground.pat_account
  - Implements updated mapping rules for prescriber_id, prescriber_key, patient_sf_id, hash_key, last_modified_date
  - Includes error handling and logging to purgo_playground.pat_account_error_log
  - All transformations and error logic are performed in a single CTE for atomicity and performance
  - Column comments document transformation rules and valid values
  - Data types and nullability conform to Databricks best practices
  - No CHECK constraints (Databricks SQL does not support)
  - All string fields use STRING, last_modified_date uses TIMESTAMP
  - All table references are fully qualified
*/

/* ------------------ DDL: Create/Replace Target Table with Comments ------------------ */
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

COMMENT ON COLUMN purgo_playground.pat_account.patient_foundation_shipment IS
  'Straight move from source. Source: p_account.patient_foundation_shipment';

COMMENT ON COLUMN purgo_playground.pat_account.prescriber_id IS
  'Convert prescriber_name_c to uppercase, prefix "DR." only if not present. Source: p_account.prescriber_name_c';

COMMENT ON COLUMN purgo_playground.pat_account.prescriber_key IS
  'Convert prescriber_name_c_address to uppercase, prefix "DR." only if not present. Source: p_account.prescriber_name_c_address';

COMMENT ON COLUMN purgo_playground.pat_account.patient_sf_id IS
  'Use value only if patinet_c contains "PAT"; otherwise set to NULL. Source: p_account.patinet_c';

COMMENT ON COLUMN purgo_playground.pat_account.service_request_type IS
  'Straight move from source. Source: p_account.recordtypeid';

COMMENT ON COLUMN purgo_playground.pat_account.case_sf_id IS
  'Straight move from source. Source: p_account.id';

COMMENT ON COLUMN purgo_playground.pat_account.account_id IS
  'Straight move from source. Source: p_account.accountid';

COMMENT ON COLUMN purgo_playground.pat_account.hash_key IS
  'Concatenation of patinet_c, recordtypeid, id with underscore (_). NULL if any source column is NULL.';

COMMENT ON COLUMN purgo_playground.pat_account.last_modified_date IS
  'Set to current timestamp at transformation. TIMESTAMP type.';

/* ------------------ DDL: Create/Replace Error Log Table ------------------ */
CREATE TABLE IF NOT EXISTS purgo_playground.pat_account_error_log (
    hash_key STRING,
    error_col STRING,
    error_message STRING,
    event_time TIMESTAMP
);

/* ------------------ CTE: Transformation and Error Detection ------------------ */
WITH transformed_pat_account AS (
  SELECT
    -- patient_foundation_shipment: straight move
    p.patient_foundation_shipment AS patient_foundation_shipment,

    -- prescriber_id: uppercase, prefix DR. if not present, NULL if source is NULL/empty
    CASE
      WHEN p.prescriber_name_c IS NULL OR TRIM(p.prescriber_name_c) = '' THEN NULL
      WHEN UPPER(p.prescriber_name_c) LIKE 'DR.%' THEN UPPER(p.prescriber_name_c)
      ELSE CONCAT('DR.', UPPER(p.prescriber_name_c))
    END AS prescriber_id,

    -- prescriber_key: uppercase, prefix DR. if not present, NULL if source is NULL/empty
    CASE
      WHEN p.prescriber_name_c_address IS NULL OR TRIM(p.prescriber_name_c_address) = '' THEN NULL
      WHEN UPPER(p.prescriber_name_c_address) LIKE 'DR.%' THEN UPPER(p.prescriber_name_c_address)
      ELSE CONCAT('DR.', UPPER(p.prescriber_name_c_address))
    END AS prescriber_key,

    -- patient_sf_id: only if patinet_c contains 'PAT', else NULL
    CASE
      WHEN p.patinet_c IS NULL OR NOT (UPPER(p.patinet_c) LIKE '%PAT%') THEN NULL
      ELSE p.patinet_c
    END AS patient_sf_id,

    -- service_request_type: straight move
    p.recordtypeid AS service_request_type,

    -- case_sf_id: straight move
    p.id AS case_sf_id,

    -- account_id: straight move
    p.accountid AS account_id,

    -- hash_key: concat patinet_c, recordtypeid, id with underscore, NULL if any source column is NULL
    CASE
      WHEN p.patinet_c IS NULL OR p.recordtypeid IS NULL OR p.id IS NULL THEN NULL
      ELSE CONCAT(p.patinet_c, '_', p.recordtypeid, '_', p.id)
    END AS hash_key,

    -- last_modified_date: current timestamp
    CURRENT_TIMESTAMP() AS last_modified_date

  FROM purgo_playground.p_account p
),

error_pat_account AS (
  -- Error: prescriber_id NULL or empty
  SELECT
    t.hash_key,
    'prescriber_id' AS error_col,
    'prescriber_name_c is NULL or empty' AS error_message,
    CURRENT_TIMESTAMP() AS event_time
  FROM transformed_pat_account t
  WHERE t.prescriber_id IS NULL

  UNION ALL

  -- Error: prescriber_key NULL or empty
  SELECT
    t.hash_key,
    'prescriber_key' AS error_col,
    'prescriber_name_c_address is NULL or empty' AS error_message,
    CURRENT_TIMESTAMP() AS event_time
  FROM transformed_pat_account t
  WHERE t.prescriber_key IS NULL

  UNION ALL

  -- Error: patient_sf_id NULL (patinet_c is NULL or does not contain 'PAT')
  SELECT
    t.hash_key,
    'patient_sf_id' AS error_col,
    'patinet_c is NULL or does not contain "PAT"' AS error_message,
    CURRENT_TIMESTAMP() AS event_time
  FROM transformed_pat_account t
  WHERE t.patient_sf_id IS NULL

  UNION ALL

  -- Error: hash_key NULL (one or more source columns for hash_key are NULL)
  SELECT
    t.hash_key,
    'hash_key' AS error_col,
    'One or more source columns for hash_key are NULL' AS error_message,
    CURRENT_TIMESTAMP() AS event_time
  FROM transformed_pat_account t
  WHERE t.hash_key IS NULL

  UNION ALL

  -- Error: last_modified_date cannot be set (should never happen, but for completeness)
  SELECT
    t.hash_key,
    'last_modified_date' AS error_col,
    'Failed to set last_modified_date' AS error_message,
    CURRENT_TIMESTAMP() AS event_time
  FROM transformed_pat_account t
  WHERE t.last_modified_date IS NULL
)

/* ------------------ DML: Insert Transformed Data ------------------ */
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
FROM transformed_pat_account;

/* ------------------ DML: Insert Error Log Data ------------------ */
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
FROM error_pat_account;

/* ------------------ END OF SCRIPT ------------------ */

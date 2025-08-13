USE CATALOG purgo_databricks;

/* 
  Enhanced SQL Transformation for purgo_playground.pat_account
  - Updates transformation logic for prescriber_id, patient_sf_id, hash_key, last_modified_date
  - Implements error logging for invalid data
  - All logic aligns with source_target_mapping_specifications.xlsx
  - Catalog: purgo_databricks
  - Schema: purgo_playground
*/

/* ------------------ DDL: Create/Update Target Table ------------------ */
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

COMMENT ON COLUMN purgo_playground.pat_account.patient_foundation_shipment IS 'Straight move from source';
COMMENT ON COLUMN purgo_playground.pat_account.prescriber_id IS 'Convert name to uppercase, then prefix "DR." only if not present. Valid values: Uppercase, prefixed with "DR."';
COMMENT ON COLUMN purgo_playground.pat_account.prescriber_key IS 'If "Dr." not in prefix, include "DR." in prefix from case in prescriber_name_c_address. Valid values: Uppercase, prefixed with "DR."';
COMMENT ON COLUMN purgo_playground.pat_account.patient_sf_id IS 'Use value only if patinet_c contains "PAT"; otherwise set to NULL';
COMMENT ON COLUMN purgo_playground.pat_account.service_request_type IS 'Straight move from source';
COMMENT ON COLUMN purgo_playground.pat_account.case_sf_id IS 'Straight move from source';
COMMENT ON COLUMN purgo_playground.pat_account.account_id IS 'Straight move from source';
COMMENT ON COLUMN purgo_playground.pat_account.hash_key IS 'Concatenation of patinet_c, recordtypeid, id with underscore (_)';
COMMENT ON COLUMN purgo_playground.pat_account.last_modified_date IS 'Set to ETL execution timestamp in ISO 8601 format';

/* ------------------ DDL: Error Log Table ------------------ */
CREATE TABLE IF NOT EXISTS purgo_playground.pat_account_error_log (
  hash_key STRING,
  error_col STRING NOT NULL,
  error_message STRING NOT NULL,
  event_time TIMESTAMP
);

/* ------------------ CTE: Source Data Transformation ------------------ */
WITH transformed_pat_account AS (
  SELECT
    -- patient_foundation_shipment: straight move
    p.patient_foundation_shipment AS patient_foundation_shipment,

    /* prescriber_id transformation:
       - Convert to uppercase
       - Prefix "DR." only if not present (case-insensitive)
       - No duplicate prefix
       - NULL if prescriber_name_c is NULL
    */
    CASE
      WHEN p.prescriber_name_c IS NULL THEN NULL
      WHEN UPPER(TRIM(p.prescriber_name_c)) LIKE 'DR.%' THEN UPPER(TRIM(p.prescriber_name_c))
      ELSE CONCAT('DR.', UPPER(TRIM(p.prescriber_name_c)))
    END AS prescriber_id,

    /* prescriber_key transformation:
       - Convert to uppercase
       - Prefix "DR." only if not present (case-insensitive)
       - NULL if prescriber_name_c_address is NULL
    */
    CASE
      WHEN p.prescriber_name_c_address IS NULL THEN NULL
      WHEN UPPER(TRIM(p.prescriber_name_c_address)) LIKE 'DR.%' THEN UPPER(TRIM(p.prescriber_name_c_address))
      ELSE CONCAT('DR.', UPPER(TRIM(p.prescriber_name_c_address)))
    END AS prescriber_key,

    /* patient_sf_id transformation:
       - Use value only if patinet_c contains 'PAT'
       - NULL if patinet_c is NULL or does not contain 'PAT'
    */
    CASE
      WHEN p.patinet_c IS NULL THEN NULL
      WHEN p.patinet_c LIKE '%PAT%' THEN p.patinet_c
      ELSE NULL
    END AS patient_sf_id,

    -- service_request_type: straight move
    p.recordtypeid AS service_request_type,

    -- case_sf_id: straight move
    p.id AS case_sf_id,

    -- account_id: straight move
    p.accountid AS account_id,

    /* hash_key transformation:
       - Concatenate patinet_c, recordtypeid, id with underscore (_)
       - NULL if any of the three is NULL
    */
    CASE
      WHEN p.patinet_c IS NULL OR p.recordtypeid IS NULL OR p.id IS NULL THEN NULL
      ELSE CONCAT(p.patinet_c, '_', p.recordtypeid, '_', p.id)
    END AS hash_key,

    /* last_modified_date transformation:
       - Set to ETL execution timestamp
       - NULL if ETL timestamp is not available (should not happen in Databricks)
    */
    CURRENT_TIMESTAMP() AS last_modified_date

  FROM purgo_playground.p_account p
)

/* ------------------ INSERT: Main Data Load ------------------ */
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

/* ------------------ INSERT: Error Logging for Invalid Data ------------------ */
-- Log errors for prescriber_id, patient_sf_id, hash_key, last_modified_date
INSERT INTO purgo_playground.pat_account_error_log
SELECT
  t.hash_key,
  t.error_col,
  t.error_message,
  CURRENT_TIMESTAMP() AS event_time
FROM (
  SELECT
    hash_key,
    -- Error for prescriber_id
    CASE WHEN prescriber_id IS NULL THEN 'prescriber_id'
         WHEN patient_sf_id IS NULL THEN 'patient_sf_id'
         WHEN hash_key IS NULL THEN 'hash_key'
         WHEN last_modified_date IS NULL THEN 'last_modified_date'
         ELSE NULL END AS error_col,
    CASE WHEN prescriber_id IS NULL THEN 'prescriber_name_c is NULL'
         WHEN patient_sf_id IS NULL THEN 'patinet_c is NULL or does not contain "PAT"'
         WHEN hash_key IS NULL THEN 'recordtypeid or id or patinet_c is NULL'
         WHEN last_modified_date IS NULL THEN 'ETL execution timestamp missing'
         ELSE NULL END AS error_message
  FROM transformed_pat_account
) t
WHERE t.error_col IS NOT NULL;

/* ------------------ END OF SCRIPT ------------------ */

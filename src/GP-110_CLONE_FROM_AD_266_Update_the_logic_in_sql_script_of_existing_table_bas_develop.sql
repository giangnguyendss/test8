/* 
================================================================================
Databricks SQL Transformation Logic Enhancement for purgo_playground.pat_account
================================================================================

This script updates the transformation logic for the following columns:
- prescriber_id: Convert to uppercase, prefix "DR." if not present (case-insensitive)
- patient_sf_id: Use value only if patinet_c contains "PAT" (case-insensitive), else NULL
- hash_key: Concatenate patinet_c, recordtypeid, id with underscores, NULLs handled as empty string
- last_modified_date: Set to current timestamp

All changes align with the latest business requirements and mapping rules in source_target_mapping_specifications.xlsx.
================================================================================
*/

USE CATALOG purgo_databricks;

/* ---------------------------------------------------------------------------
-- DDL: Create target table with updated schema (includes last_modified_date)
--------------------------------------------------------------------------- */
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

/* ---------------------------------------------------------------------------
-- DML: Insert transformed data from source table with updated logic
--------------------------------------------------------------------------- */
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
WITH source_data AS (
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
transformed_data AS (
    SELECT
        /* Straight move from source */
        patient_foundation_shipment AS patient_foundation_shipment,

        /* prescriber_id: Convert to uppercase, prefix "DR." if not present (case-insensitive) */
        CASE 
            WHEN prescriber_name_c IS NULL THEN NULL
            WHEN UPPER(prescriber_name_c) LIKE 'DR.%' THEN UPPER(prescriber_name_c)
            ELSE CONCAT('DR.', UPPER(prescriber_name_c))
        END AS prescriber_id,

        /* prescriber_key: If "Dr." not in prefix, include "DR." in prefix from case in prescriber_name_c_address */
        CASE 
            WHEN prescriber_name_c_address IS NULL THEN NULL
            WHEN UPPER(prescriber_name_c_address) LIKE 'DR.%' THEN UPPER(prescriber_name_c_address)
            ELSE CONCAT('DR.', UPPER(prescriber_name_c_address))
        END AS prescriber_key,

        /* patient_sf_id: Use value only if patinet_c contains "PAT" (case-insensitive), else NULL */
        CASE 
            WHEN patinet_c IS NULL THEN NULL
            WHEN UPPER(patinet_c) LIKE '%PAT%' THEN patinet_c
            ELSE NULL
        END AS patient_sf_id,

        /* service_request_type: straight move from source */
        recordtypeid AS service_request_type,

        /* case_sf_id: straight move from source */
        id AS case_sf_id,

        /* account_id: straight move from source */
        accountid AS account_id,

        /* hash_key: Concatenate patinet_c, recordtypeid, id with underscores, NULLs handled as empty string */
        CONCAT(
            COALESCE(patinet_c, ''),
            '_',
            COALESCE(recordtypeid, ''),
            '_',
            COALESCE(id, '')
        ) AS hash_key,

        /* last_modified_date: current timestamp */
        CURRENT_TIMESTAMP() AS last_modified_date
    FROM source_data
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
FROM transformed_data;

/* ---------------------------------------------------------------------------
-- Validation Query: CTE for transformed data (for downstream validation)
--------------------------------------------------------------------------- */
WITH validation_cte AS (
    SELECT
        patient_foundation_shipment AS patient_foundation_shipment,
        /* prescriber_id transformation validation */
        CASE 
            WHEN prescriber_name_c IS NULL THEN NULL
            WHEN UPPER(prescriber_name_c) LIKE 'DR.%' THEN UPPER(prescriber_name_c)
            ELSE CONCAT('DR.', UPPER(prescriber_name_c))
        END AS prescriber_id,
        /* prescriber_key transformation validation */
        CASE 
            WHEN prescriber_name_c_address IS NULL THEN NULL
            WHEN UPPER(prescriber_name_c_address) LIKE 'DR.%' THEN UPPER(prescriber_name_c_address)
            ELSE CONCAT('DR.', UPPER(prescriber_name_c_address))
        END AS prescriber_key,
        /* patient_sf_id transformation validation */
        CASE 
            WHEN patinet_c IS NULL THEN NULL
            WHEN UPPER(patinet_c) LIKE '%PAT%' THEN patinet_c
            ELSE NULL
        END AS patient_sf_id,
        recordtypeid AS service_request_type,
        id AS case_sf_id,
        accountid AS account_id,
        /* hash_key transformation validation */
        CONCAT(
            COALESCE(patinet_c, ''),
            '_',
            COALESCE(recordtypeid, ''),
            '_',
            COALESCE(id, '')
        ) AS hash_key,
        /* last_modified_date validation */
        CURRENT_TIMESTAMP() AS last_modified_date
    FROM purgo_playground.p_account
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
FROM validation_cte;

/* ---------------------------------------------------------------------------
-- End of Transformation Script
--------------------------------------------------------------------------- */


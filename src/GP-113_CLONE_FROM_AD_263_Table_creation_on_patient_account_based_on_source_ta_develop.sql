/* 
    Databricks SQL Implementation for Patient Account Table Creation and Data Transformation
    --------------------------------------------------------------------------------------
    - Catalog: purgo_databricks
    - Schema: purgo_playground
    - Target Table: patient_account
    - Source Table: pa_account
    - Mapping Spec: source_target_mapping_specification (1).xlsx
    - Implements all transformation, filtering, error handling, and schema requirements
*/

/* -------------------------------------------------------------------------------
   SECTION: Setup & Cleanup
   ------------------------------------------------------------------------------- */

-- Use correct Unity Catalog
USE CATALOG purgo_databricks;

-- Drop target table if exists
DROP TABLE IF EXISTS purgo_databricks.purgo_playground.patient_account;

-- Drop error log table if exists
DROP TABLE IF EXISTS purgo_databricks.purgo_playground.pat_account_error_log;

/* -------------------------------------------------------------------------------
   SECTION: Table Creation (Schema, Constraints, Documentation)
   ------------------------------------------------------------------------------- */

CREATE TABLE purgo_databricks.purgo_playground.patient_account (
    patient_foundation_shipment STRING NOT NULL COMMENT "Straight move from Source",
    prescriber_id STRING NOT NULL COMMENT "If \"Dr.\" not in prefix, Include \"DR.\" in prefix from pa_account in prescriber_name_c",
    prescriber_key STRING NOT NULL COMMENT "If \"Dr.\" not in prefix, Include \"DR.\" in prefix from pa_account in prescriber_name_c_address",
    patient_sf_id STRING NOT NULL COMMENT "Straight move from Source",
    service_request_type STRING NOT NULL COMMENT "Straight move from Source",
    case_sf_id STRING NOT NULL COMMENT "Straight move from Source",
    account_id STRING NOT NULL COMMENT "Straight move from Source",
    hash_key STRING NOT NULL COMMENT "Concatenation of value (patinet_c#recordtypeid#id#accountid)",
    last_modified_date TIMESTAMP NOT NULL COMMENT "Current timestamp (ETL run time)",
    planned_date DATE NULL COMMENT "No mapping provided, set to NULL"
);

CREATE TABLE purgo_databricks.purgo_playground.pat_account_error_log (
    hash_key STRING,
    error_col STRING NOT NULL,
    error_message STRING NOT NULL,
    event_time TIMESTAMP
);

/* -------------------------------------------------------------------------------
   SECTION: ETL Insert (Transformation, Filtering, Data Quality, Uniqueness)
   ------------------------------------------------------------------------------- */

-- Insert valid records from pa_account into patient_account
INSERT INTO purgo_databricks.purgo_playground.patient_account
SELECT
    pa.patient_foundation_shipment,
    CASE
        WHEN pa.prescriber_name_c IS NULL THEN NULL
        WHEN LOWER(TRIM(pa.prescriber_name_c)) LIKE 'dr.%' THEN pa.prescriber_name_c
        ELSE CONCAT('DR. ', pa.prescriber_name_c)
    END AS prescriber_id,
    CASE
        WHEN pa.prescriber_name_c_address IS NULL THEN NULL
        WHEN LOWER(TRIM(pa.prescriber_name_c_address)) LIKE 'dr.%' THEN pa.prescriber_name_c_address
        ELSE CONCAT('DR. ', pa.prescriber_name_c_address)
    END AS prescriber_key,
    pa.patinet_c AS patient_sf_id,
    pa.recordtypeid AS service_request_type,
    pa.id AS case_sf_id,
    pa.accountid AS account_id,
    CONCAT(pa.patinet_c, '#', pa.recordtypeid, '#', pa.id, '#', pa.accountid) AS hash_key,
    CURRENT_TIMESTAMP() AS last_modified_date,
    NULL AS planned_date
FROM purgo_databricks.purgo_playground.pa_account pa
WHERE
    -- Only Salesforce records
    pa.patinet_c IS NOT NULL
    AND LOWER(pa.patinet_c) LIKE '%pat%'
    -- All NOT NULL columns must be present
    AND pa.patient_foundation_shipment IS NOT NULL
    AND pa.prescriber_name_c IS NOT NULL
    AND pa.prescriber_name_c_address IS NOT NULL
    AND pa.recordtypeid IS NOT NULL
    AND pa.id IS NOT NULL
    AND pa.accountid IS NOT NULL
    -- Data type validation: all fields are string
    AND typeof(pa.patient_foundation_shipment) = 'string'
    AND typeof(pa.prescriber_name_c) = 'string'
    AND typeof(pa.prescriber_name_c_address) = 'string'
    AND typeof(pa.patinet_c) = 'string'
    AND typeof(pa.recordtypeid) = 'string'
    AND typeof(pa.id) = 'string'
    AND typeof(pa.accountid) = 'string'
    -- Uniqueness: skip duplicate hash_key
    AND CONCAT(pa.patinet_c, '#', pa.recordtypeid, '#', pa.id, '#', pa.accountid) NOT IN (
        SELECT hash_key FROM purgo_databricks.purgo_playground.patient_account
    );

/* -------------------------------------------------------------------------------
   SECTION: Error Logging (Skipped Records: Filter, NULL, Duplicate, Type Mismatch)
   ------------------------------------------------------------------------------- */

-- Error: patinet_c does not contain "PAT"
INSERT INTO purgo_databricks.purgo_playground.pat_account_error_log
SELECT
    CONCAT(pa.patinet_c, '#', pa.recordtypeid, '#', pa.id, '#', pa.accountid) AS hash_key,
    'patinet_c' AS error_col,
    'patinet_c does not contain "PAT"' AS error_message,
    CURRENT_TIMESTAMP() AS event_time
FROM purgo_databricks.purgo_playground.pa_account pa
WHERE
    (pa.patinet_c IS NULL OR LOWER(pa.patinet_c) NOT LIKE '%pat%')
    AND pa.patient_foundation_shipment IS NOT NULL
    AND pa.prescriber_name_c IS NOT NULL
    AND pa.prescriber_name_c_address IS NOT NULL
    AND pa.recordtypeid IS NOT NULL
    AND pa.id IS NOT NULL
    AND pa.accountid IS NOT NULL
    AND typeof(pa.patient_foundation_shipment) = 'string'
    AND typeof(pa.prescriber_name_c) = 'string'
    AND typeof(pa.prescriber_name_c_address) = 'string'
    AND typeof(pa.recordtypeid) = 'string'
    AND typeof(pa.id) = 'string'
    AND typeof(pa.accountid) = 'string';

-- Error: NULL in NOT NULL columns
INSERT INTO purgo_databricks.purgo_playground.pat_account_error_log
SELECT
    CONCAT(pa.patinet_c, '#', pa.recordtypeid, '#', pa.id, '#', pa.accountid) AS hash_key,
    error_col,
    CONCAT(error_col, ' is NULL for NOT NULL target column') AS error_message,
    CURRENT_TIMESTAMP() AS event_time
FROM purgo_databricks.purgo_playground.pa_account pa
LATERAL VIEW EXPLODE(
    ARRAY(
        IF(pa.patient_foundation_shipment IS NULL, 'patient_foundation_shipment', NULL),
        IF(pa.prescriber_name_c IS NULL, 'prescriber_name_c', NULL),
        IF(pa.prescriber_name_c_address IS NULL, 'prescriber_name_c_address', NULL),
        IF(pa.recordtypeid IS NULL, 'recordtypeid', NULL),
        IF(pa.id IS NULL, 'id', NULL),
        IF(pa.accountid IS NULL, 'accountid', NULL)
    )
) t AS error_col
WHERE
    pa.patinet_c IS NOT NULL
    AND LOWER(pa.patinet_c) LIKE '%pat%'
    AND (
        pa.patient_foundation_shipment IS NULL
        OR pa.prescriber_name_c IS NULL
        OR pa.prescriber_name_c_address IS NULL
        OR pa.recordtypeid IS NULL
        OR pa.id IS NULL
        OR pa.accountid IS NULL
    );

-- Error: Data type mismatch
INSERT INTO purgo_databricks.purgo_playground.pat_account_error_log
SELECT
    CONCAT(pa.patinet_c, '#', pa.recordtypeid, '#', pa.id, '#', pa.accountid) AS hash_key,
    col.error_col,
    CONCAT('Data type mismatch for ', col.error_col, ': ', col.invalid_value) AS error_message,
    CURRENT_TIMESTAMP() AS event_time
FROM purgo_databricks.purgo_playground.pa_account pa
LATERAL VIEW EXPLODE(
    ARRAY(
        IF(typeof(pa.patient_foundation_shipment) != 'string', named_struct('error_col', 'patient_foundation_shipment', 'invalid_value', pa.patient_foundation_shipment), NULL),
        IF(typeof(pa.prescriber_name_c) != 'string', named_struct('error_col', 'prescriber_name_c', 'invalid_value', pa.prescriber_name_c), NULL),
        IF(typeof(pa.prescriber_name_c_address) != 'string', named_struct('error_col', 'prescriber_name_c_address', 'invalid_value', pa.prescriber_name_c_address), NULL),
        IF(typeof(pa.patinet_c) != 'string', named_struct('error_col', 'patinet_c', 'invalid_value', pa.patinet_c), NULL),
        IF(typeof(pa.recordtypeid) != 'string', named_struct('error_col', 'recordtypeid', 'invalid_value', pa.recordtypeid), NULL),
        IF(typeof(pa.id) != 'string', named_struct('error_col', 'id', 'invalid_value', pa.id), NULL),
        IF(typeof(pa.accountid) != 'string', named_struct('error_col', 'accountid', 'invalid_value', pa.accountid), NULL)
    )
) col
WHERE
    pa.patinet_c IS NOT NULL
    AND LOWER(pa.patinet_c) LIKE '%pat%'
    AND pa.patient_foundation_shipment IS NOT NULL
    AND pa.prescriber_name_c IS NOT NULL
    AND pa.prescriber_name_c_address IS NOT NULL
    AND pa.recordtypeid IS NOT NULL
    AND pa.id IS NOT NULL
    AND pa.accountid IS NOT NULL;

-- Error: Duplicate hash_key
INSERT INTO purgo_databricks.purgo_playground.pat_account_error_log
SELECT
    hash_key,
    'hash_key' AS error_col,
    'Duplicate hash_key value' AS error_message,
    CURRENT_TIMESTAMP() AS event_time
FROM (
    SELECT
        CONCAT(pa.patinet_c, '#', pa.recordtypeid, '#', pa.id, '#', pa.accountid) AS hash_key,
        COUNT(*) AS cnt
    FROM purgo_databricks.purgo_playground.pa_account pa
    WHERE
        pa.patinet_c IS NOT NULL
        AND LOWER(pa.patinet_c) LIKE '%pat%'
        AND pa.patient_foundation_shipment IS NOT NULL
        AND pa.prescriber_name_c IS NOT NULL
        AND pa.prescriber_name_c_address IS NOT NULL
        AND pa.recordtypeid IS NOT NULL
        AND pa.id IS NOT NULL
        AND pa.accountid IS NOT NULL
        AND typeof(pa.patient_foundation_shipment) = 'string'
        AND typeof(pa.prescriber_name_c) = 'string'
        AND typeof(pa.prescriber_name_c_address) = 'string'
        AND typeof(pa.patinet_c) = 'string'
        AND typeof(pa.recordtypeid) = 'string'
        AND typeof(pa.id) = 'string'
        AND typeof(pa.accountid) = 'string'
    GROUP BY CONCAT(pa.patinet_c, '#', pa.recordtypeid, '#', pa.id, '#', pa.accountid)
    HAVING cnt > 1
);

/* -------------------------------------------------------------------------------
   SECTION: Validation Query (CTE)
   ------------------------------------------------------------------------------- */

WITH patient_account_cte AS (
    SELECT * FROM purgo_databricks.purgo_playground.patient_account
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
    last_modified_date,
    planned_date
FROM patient_account_cte;


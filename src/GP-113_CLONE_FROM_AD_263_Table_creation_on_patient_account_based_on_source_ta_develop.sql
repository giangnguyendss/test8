/*
    -------------------------------------------------------------------------------
    Patient Account Table Creation and Data Transformation
    -------------------------------------------------------------------------------
    - Catalog: purgo_databricks
    - Schema: purgo_playground
    - Target Table: patient_account
    - Source Table: pa_account
    - Mapping Spec: source_target_mapping_specification (1).xlsx
    - Implements all transformation, filtering, and error handling as per requirements
    -------------------------------------------------------------------------------
*/

/* SECTION: Setup & Cleanup */
/* Set catalog and drop target table if exists */
USE CATALOG purgo_databricks;

DROP TABLE IF EXISTS purgo_databricks.purgo_playground.patient_account;

/* SECTION: Table Creation */
/* Create patient_account table with required schema */
CREATE TABLE purgo_databricks.purgo_playground.patient_account (
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

/* SECTION: Data Transformation & Load */
/* Insert transformed data from pa_account into patient_account */
INSERT INTO purgo_databricks.purgo_playground.patient_account
SELECT
    /* patient_foundation_shipment: straight move, truncate to 255 if needed */
    CASE WHEN patient_foundation_shipment IS NOT NULL THEN LEFT(patient_foundation_shipment, 255) ELSE NULL END AS patient_foundation_shipment,

    /* prescriber_id: If not starts with "Dr." (case-insensitive), prepend "DR. " else straight move, truncate to 18 */
    CASE
        WHEN prescriber_name_c IS NULL THEN NULL
        WHEN LOWER(TRIM(prescriber_name_c)) LIKE 'dr.%' THEN LEFT(prescriber_name_c, 18)
        ELSE LEFT(CONCAT('DR. ', prescriber_name_c), 18)
    END AS prescriber_id,

    /* prescriber_key: same logic as prescriber_id, truncate to 18 */
    CASE
        WHEN prescriber_name_c_address IS NULL THEN NULL
        WHEN LOWER(TRIM(prescriber_name_c_address)) LIKE 'dr.%' THEN LEFT(prescriber_name_c_address, 18)
        ELSE LEFT(CONCAT('DR. ', prescriber_name_c_address), 18)
    END AS prescriber_key,

    /* patient_sf_id: straight move from patinet_c, truncate to 18 */
    CASE WHEN patinet_c IS NOT NULL THEN LEFT(patinet_c, 18) ELSE NULL END AS patient_sf_id,

    /* service_request_type: straight move from recordtypeid, truncate to 255 */
    CASE WHEN recordtypeid IS NOT NULL THEN LEFT(recordtypeid, 255) ELSE NULL END AS service_request_type,

    /* case_sf_id: straight move from id, truncate to 18 */
    CASE WHEN id IS NOT NULL THEN LEFT(id, 18) ELSE NULL END AS case_sf_id,

    /* account_id: straight move from accountid, truncate to 18 */
    CASE WHEN accountid IS NOT NULL THEN LEFT(accountid, 18) ELSE NULL END AS account_id,

    /* hash_key: concatenate patinet_c#recordtypeid#id#accountid, NULL if any is NULL */
    CASE
        WHEN patinet_c IS NULL OR recordtypeid IS NULL OR id IS NULL OR accountid IS NULL THEN NULL
        ELSE CONCAT(LEFT(patinet_c, 18), '#', LEFT(recordtypeid, 255), '#', LEFT(id, 18), '#', LEFT(accountid, 18))
    END AS hash_key,

    /* last_modified_date: current timestamp */
    CURRENT_TIMESTAMP() AS last_modified_date

FROM purgo_databricks.purgo_playground.pa_account
/* Only include records where patinet_c contains 'PAT' (case-insensitive, anywhere in string) */
WHERE patinet_c IS NOT NULL AND LOWER(patinet_c) LIKE '%pat%';

/* SECTION: Validation Query */
/* Validate loaded data using CTE */
WITH validation AS (
    SELECT
        COUNT(*) AS total_records,
        COUNT(DISTINCT hash_key) AS distinct_hash_keys,
        COUNT_IF(patient_sf_id IS NULL) AS null_patient_sf_id,
        COUNT_IF(prescriber_id IS NULL) AS null_prescriber_id,
        COUNT_IF(prescriber_key IS NULL) AS null_prescriber_key,
        COUNT_IF(patient_foundation_shipment IS NULL) AS null_patient_foundation_shipment,
        COUNT_IF(account_id IS NULL) AS null_account_id,
        COUNT_IF(LENGTH(account_id) > 18) AS account_id_overflow,
        COUNT_IF(hash_key IS NULL) AS null_hash_key
    FROM purgo_databricks.purgo_playground.patient_account
)
SELECT * FROM validation;

/* End of implementation */


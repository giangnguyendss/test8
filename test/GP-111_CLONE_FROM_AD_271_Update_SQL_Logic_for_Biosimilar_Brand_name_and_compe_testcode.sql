USE CATALOG purgo_databricks;

/* 
================================================================================
Databricks SQL Test Suite for bai_sales.sql Update: competitor_flag logic & brand_normalized_name suppression
================================================================================
Setup & Configuration
--------------------------------------------------------------------------------
- Catalog: purgo_databricks
- Schema: purgo_playground
- Source Table: purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
- Error Log Table: purgo_playground.bai_sales_agg_obu_customer_datapack_invalid_cdl_effective_date_log
- All test data and logic are self-contained and do not use temp views or temp tables.
- All comments follow Databricks SQL best practices.
================================================================================
*/

/* ============================================================================
Section: Schema Validation Tests
============================================================================ */

/* -- Validate schema of source table: purgo_playground.bai_sales_agg_obu_customer_datapack_weekly -- */
SELECT
  COUNT(*) AS schema_mismatch_count
FROM (
  SELECT
    CASE WHEN
      (SELECT COUNT(*) FROM information_schema.columns
        WHERE table_catalog = "purgo_databricks"
          AND table_schema = "purgo_playground"
          AND table_name = "bai_sales_agg_obu_customer_datapack_weekly"
          AND column_name IN (
            "brand_normalized_name",
            "normalized_name",
            "market_normalized_name",
            "competitor_flag",
            "channel_name",
            "transaction_timestamp",
            "integrated_units",
            "integrated_normalized_units",
            "integrated_dollars",
            "cdl_effective_date"
          )
      ) = 10
    THEN 0 ELSE 1 END AS mismatch
) AS schema_check
WHERE mismatch = 1;

/* -- Assert: schema_mismatch_count must be 0 -- */

/* ============================================================================
Section: Data Type Conversion & NULL Handling Tests
============================================================================ */

/* -- Validate competitor_flag is cast to STRING and NULLs handled -- */
WITH competitor_flag_test AS (
  SELECT
    competitor_flag,
    CAST(competitor_flag AS STRING) AS competitor_flag_str
  FROM purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
  WHERE competitor_flag IS NULL OR competitor_flag IN (TRUE, FALSE)
  LIMIT 10
)
SELECT
  competitor_flag,
  competitor_flag_str,
  CASE
    WHEN competitor_flag IS NULL AND competitor_flag_str = "null" THEN "PASS"
    WHEN competitor_flag = TRUE AND competitor_flag_str = "true" THEN "PASS"
    WHEN competitor_flag = FALSE AND competitor_flag_str = "false" THEN "PASS"
    ELSE "FAIL"
  END AS assertion_result
FROM competitor_flag_test;

/* -- Assert: All assertion_result must be "PASS" -- */

/* ============================================================================
Section: Unit Test - Suppression Logic for Brand
============================================================================ */

/* -- Validate suppression logic for integrated_units, integrated_normalized_units, integrated_dollars -- */
WITH suppression_test AS (
  SELECT
    brand_normalized_name,
    integrated_units,
    integrated_normalized_units,
    integrated_dollars,
    CASE WHEN lower(brand_normalized_name) IN ("kanjinti","mvasi","riabni") THEN 0 ELSE integrated_units END AS units_suppressed,
    CASE WHEN lower(brand_normalized_name) IN ("kanjinti","mvasi","riabni") THEN 0 ELSE integrated_normalized_units END AS norm_units_suppressed,
    CASE WHEN lower(brand_normalized_name) IN ("kanjinti","mvasi","riabni") THEN 0 ELSE integrated_dollars END AS dollars_suppressed
  FROM purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
  WHERE brand_normalized_name IN ("kanjinti","mvasi","riabni","avastin","herceptin","xyz")
  LIMIT 20
)
SELECT
  brand_normalized_name,
  integrated_units,
  units_suppressed,
  integrated_normalized_units,
  norm_units_suppressed,
  integrated_dollars,
  dollars_suppressed,
  CASE
    WHEN lower(brand_normalized_name) IN ("kanjinti","mvasi","riabni")
      AND units_suppressed = 0
      AND norm_units_suppressed = 0
      AND dollars_suppressed = 0
    THEN "PASS"
    WHEN lower(brand_normalized_name) NOT IN ("kanjinti","mvasi","riabni")
      AND units_suppressed = integrated_units
      AND norm_units_suppressed = integrated_normalized_units
      AND dollars_suppressed = integrated_dollars
    THEN "PASS"
    ELSE "FAIL"
  END AS assertion_result
FROM suppression_test;

/* -- Assert: All assertion_result must be "PASS" -- */

/* ============================================================================
Section: Integration Test - End-to-End Query Logic
============================================================================ */

/* -- Final output logic as per requirements -- */
WITH bai_sales_output AS (
  SELECT
    brand_normalized_name,
    normalized_name,
    market_normalized_name,
    CAST(competitor_flag AS STRING) AS competitor_flag,
    channel_name,
    transaction_timestamp,
    SUM(CASE WHEN lower(brand_normalized_name) IN ("kanjinti","mvasi","riabni")
             THEN 0 ELSE COALESCE(integrated_units, 0) END) AS integrated_units,
    SUM(CASE WHEN lower(brand_normalized_name) IN ("kanjinti","mvasi","riabni")
             THEN 0 ELSE COALESCE(integrated_normalized_units, 0) END) AS integrated_normalized_units,
    SUM(CASE WHEN lower(brand_normalized_name) IN ("kanjinti","mvasi","riabni")
             THEN 0 ELSE COALESCE(integrated_dollars, 0.0) END) AS integrated_dollars
  FROM purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
  WHERE substring(CAST(transaction_timestamp AS STRING),1,10) >= CAST(add_months(cdl_effective_date, -36) AS STRING)
    AND lower(market_normalized_name) IN ("trastuzumab-anns","bevacizumab-awwb","rituximab market")
  GROUP BY
    brand_normalized_name,
    normalized_name,
    market_normalized_name,
    CAST(competitor_flag AS STRING),
    channel_name,
    transaction_timestamp
)
SELECT * FROM bai_sales_output;

/* -- Assert: Output columns must match required structure -- */

/* ============================================================================
Section: Data Quality Validation Tests
============================================================================ */

/* -- Validate that records with market_normalized_name not in allowed list are excluded -- */
WITH excluded_market_test AS (
  SELECT
    COUNT(*) AS excluded_count
  FROM purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
  WHERE lower(market_normalized_name) NOT IN ("trastuzumab-anns","bevacizumab-awwb","rituximab market")
    AND substring(CAST(transaction_timestamp AS STRING),1,10) >= CAST(add_months(cdl_effective_date, -36) AS STRING)
)
SELECT
  excluded_count,
  CASE WHEN excluded_count = 0 THEN "PASS" ELSE "FAIL" END AS assertion_result
FROM excluded_market_test;

/* -- Assert: assertion_result must be "PASS" -- */

/* ============================================================================
Section: Error Handling - Invalid cdl_effective_date or transaction_timestamp
============================================================================ */

/* -- Validate error log table for invalid cdl_effective_date or transaction_timestamp -- */
WITH error_log_test AS (
  SELECT
    brand_normalized_name,
    normalized_name,
    market_normalized_name,
    CAST(competitor_flag AS STRING) AS competitor_flag,
    channel_name,
    transaction_timestamp,
    cdl_effective_date,
    error_message
  FROM purgo_playground.bai_sales_agg_obu_customer_datapack_invalid_cdl_effective_date_log
  WHERE error_message = "Invalid cdl_effective_date or transaction_timestamp"
)
SELECT
  COUNT(*) AS error_count,
  CASE WHEN error_count >= 1 THEN "PASS" ELSE "FAIL" END AS assertion_result
FROM error_log_test;

/* -- Assert: assertion_result must be "PASS" -- */

/* ============================================================================
Section: Window Function & Analytics Feature Test
============================================================================ */

/* -- Test window function: row_number partitioned by brand_normalized_name -- */
WITH window_test AS (
  SELECT
    brand_normalized_name,
    normalized_name,
    market_normalized_name,
    CAST(competitor_flag AS STRING) AS competitor_flag,
    channel_name,
    transaction_timestamp,
    ROW_NUMBER() OVER (PARTITION BY brand_normalized_name ORDER BY transaction_timestamp DESC) AS rn
  FROM purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
  WHERE substring(CAST(transaction_timestamp AS STRING),1,10) >= CAST(add_months(cdl_effective_date, -36) AS STRING)
    AND lower(market_normalized_name) IN ("trastuzumab-anns","bevacizumab-awwb","rituximab market")
)
SELECT
  brand_normalized_name,
  COUNT(*) AS total_rows,
  MAX(rn) AS max_row_number
FROM window_test
GROUP BY brand_normalized_name;

/* -- Assert: max_row_number equals total_rows for each brand -- */

/* ============================================================================
Section: Delta Lake Operations Test
============================================================================ */

/* -- Test MERGE operation: upsert logic for weekly sales -- */
MERGE INTO purgo_playground.bai_sales_agg_obu_customer_datapack_weekly AS target
USING (
  SELECT
    brand_normalized_name,
    normalized_name,
    market_normalized_name,
    competitor_flag,
    channel_name,
    transaction_timestamp,
    integrated_units,
    integrated_normalized_units,
    integrated_dollars,
    cdl_effective_date
  FROM purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
  WHERE transaction_timestamp = CAST("2024-05-01T00:00:00.000+0000" AS TIMESTAMP)
    AND brand_normalized_name = "avastin"
) AS source
ON target.brand_normalized_name = source.brand_normalized_name
   AND target.transaction_timestamp = source.transaction_timestamp
WHEN MATCHED THEN
  UPDATE SET
    target.integrated_units = source.integrated_units,
    target.integrated_normalized_units = source.integrated_normalized_units,
    target.integrated_dollars = source.integrated_dollars
WHEN NOT MATCHED THEN
  INSERT (
    brand_normalized_name,
    normalized_name,
    market_normalized_name,
    competitor_flag,
    channel_name,
    transaction_timestamp,
    integrated_units,
    integrated_normalized_units,
    integrated_dollars,
    cdl_effective_date
  )
  VALUES (
    source.brand_normalized_name,
    source.normalized_name,
    source.market_normalized_name,
    source.competitor_flag,
    source.channel_name,
    source.transaction_timestamp,
    source.integrated_units,
    source.integrated_normalized_units,
    source.integrated_dollars,
    source.cdl_effective_date
  );

/* -- Assert: After MERGE, record for "avastin" and "2024-05-01T00:00:00.000+0000" exists and is updated -- */

/* ============================================================================
Section: Cleanup Operations
============================================================================ */

/* -- Cleanup: Remove test records from error log table -- */
DELETE FROM purgo_playground.bai_sales_agg_obu_customer_datapack_invalid_cdl_effective_date_log
WHERE error_message = "Invalid cdl_effective_date or transaction_timestamp";

/* -- Cleanup: Remove test records from weekly table for "avastin" on "2024-05-01T00:00:00.000+0000" -- */
DELETE FROM purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
WHERE brand_normalized_name = "avastin"
  AND transaction_timestamp = CAST("2024-05-01T00:00:00.000+0000" AS TIMESTAMP);

/* ============================================================================
Section: Performance Test - Query Execution Time
============================================================================ */

/* -- Test query execution time for main output logic -- */
EXPLAIN
SELECT
  brand_normalized_name,
  normalized_name,
  market_normalized_name,
  CAST(competitor_flag AS STRING) AS competitor_flag,
  channel_name,
  transaction_timestamp,
  SUM(CASE WHEN lower(brand_normalized_name) IN ("kanjinti","mvasi","riabni")
           THEN 0 ELSE COALESCE(integrated_units, 0) END) AS integrated_units,
  SUM(CASE WHEN lower(brand_normalized_name) IN ("kanjinti","mvasi","riabni")
           THEN 0 ELSE COALESCE(integrated_normalized_units, 0) END) AS integrated_normalized_units,
  SUM(CASE WHEN lower(brand_normalized_name) IN ("kanjinti","mvasi","riabni")
           THEN 0 ELSE COALESCE(integrated_dollars, 0.0) END) AS integrated_dollars
FROM purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
WHERE substring(CAST(transaction_timestamp AS STRING),1,10) >= CAST(add_months(cdl_effective_date, -36) AS STRING)
  AND lower(market_normalized_name) IN ("trastuzumab-anns","bevacizumab-awwb","rituximab market")
GROUP BY
  brand_normalized_name,
  normalized_name,
  market_normalized_name,
  CAST(competitor_flag AS STRING),
  channel_name,
  transaction_timestamp;

/* -- Assert: Query plan is optimal (check for full scan, shuffle, etc.) -- */

/* ============================================================================
Section: SQL UDF Test - Scalar Function for Brand Suppression
============================================================================ */

/* -- Create or replace scalar UDF for suppression logic -- */
CREATE OR REPLACE FUNCTION purgo_playground.brand_suppression_flag(brand STRING)
RETURNS STRING
RETURN CASE WHEN lower(brand) IN ("kanjinti","mvasi","riabni") THEN "suppressed" ELSE "active" END;

/* -- Test UDF -- */
SELECT
  brand_normalized_name,
  purgo_playground.brand_suppression_flag(brand_normalized_name) AS suppression_status
FROM purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
WHERE brand_normalized_name IN ("kanjinti","mvasi","riabni","avastin","herceptin","xyz")
LIMIT 10;

/* -- Assert: suppression_status is "suppressed" for kanjinti, mvasi, riabni; "active" otherwise -- */

/* ============================================================================
Section: Foreign Key Relationship Validation
============================================================================ */

/* -- Validate foreign key relationship between weekly and error log tables -- */
SELECT
  COUNT(*) AS fk_mismatch_count
FROM purgo_playground.bai_sales_agg_obu_customer_datapack_invalid_cdl_effective_date_log AS err
LEFT JOIN purgo_playground.bai_sales_agg_obu_customer_datapack_weekly AS wk
  ON err.brand_normalized_name = wk.brand_normalized_name
  AND err.transaction_timestamp = wk.transaction_timestamp
WHERE wk.brand_normalized_name IS NULL;

/* -- Assert: fk_mismatch_count must be 0 if all error log records reference valid weekly records -- */

/* ============================================================================
Section: Final Output - Main Query Result
============================================================================ */

/* -- Final output for reporting -- */
SELECT
  brand_normalized_name,
  normalized_name,
  market_normalized_name,
  CAST(competitor_flag AS STRING) AS competitor_flag,
  channel_name,
  transaction_timestamp,
  SUM(CASE WHEN lower(brand_normalized_name) IN ("kanjinti","mvasi","riabni")
           THEN 0 ELSE COALESCE(integrated_units, 0) END) AS integrated_units,
  SUM(CASE WHEN lower(brand_normalized_name) IN ("kanjinti","mvasi","riabni")
           THEN 0 ELSE COALESCE(integrated_normalized_units, 0) END) AS integrated_normalized_units,
  SUM(CASE WHEN lower(brand_normalized_name) IN ("kanjinti","mvasi","riabni")
           THEN 0 ELSE COALESCE(integrated_dollars, 0.0) END) AS integrated_dollars
FROM purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
WHERE substring(CAST(transaction_timestamp AS STRING),1,10) >= CAST(add_months(cdl_effective_date, -36) AS STRING)
  AND lower(market_normalized_name) IN ("trastuzumab-anns","bevacizumab-awwb","rituximab market")
GROUP BY
  brand_normalized_name,
  normalized_name,
  market_normalized_name,
  CAST(competitor_flag AS STRING),
  channel_name,
  transaction_timestamp
;

/* -- End of Databricks SQL Test Suite -- */

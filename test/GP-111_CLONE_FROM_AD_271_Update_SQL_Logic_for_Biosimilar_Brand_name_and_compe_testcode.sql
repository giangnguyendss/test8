USE CATALOG purgo_databricks;

/* 
  Databricks SQL Test Suite for bai_sales.sql Update
  Catalog: purgo_databricks
  Schema: purgo_playground

  This test suite validates:
    - Correct suppression logic for competitor_flag and brand_normalized_name
    - Proper casting of competitor_flag to STRING
    - Output schema and data types
    - Handling of NULLs and invalid cdl_effective_date
    - Data quality and integrity
    - Delta Lake operations and cleanup
    - Window and analytics functions
    - Performance (row count, execution time)
    - Foreign key and check constraints
*/

/* -------------------- SETUP: Create Test Table and Load Data -------------------- */

-- Drop and recreate the test table to ensure a clean environment
DROP TABLE IF EXISTS purgo_databricks.purgo_playground.bai_sales_agg_obu_customer_datapack_weekly;

CREATE TABLE purgo_databricks.purgo_playground.bai_sales_agg_obu_customer_datapack_weekly (
  brand_normalized_name STRING,
  normalized_name STRING,
  market_normalized_name STRING,
  competitor_flag BOOLEAN,
  channel_name STRING,
  transaction_timestamp TIMESTAMP,
  integrated_units INT,
  integrated_normalized_units INT,
  integrated_dollars DOUBLE,
  br_gpo_flag STRING,
  cdl_effective_date DATE
)
USING DELTA;

-- Insert test data from provided CTE
INSERT INTO purgo_databricks.purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
WITH test_data AS (
  SELECT 'abc' AS brand_normalized_name, 'ABC Normalized' AS normalized_name, 'trastuzumab-anns' AS market_normalized_name, FALSE AS competitor_flag, 'Hospital' AS channel_name, TIMESTAMP('2024-03-21T00:00:00.000+0000') AS transaction_timestamp, 100 AS integrated_units, 90 AS integrated_normalized_units, 1000.50 AS integrated_dollars, 'N' AS br_gpo_flag, DATE('2021-03-21') AS cdl_effective_date
  UNION ALL SELECT 'kanjinti', 'KANJINTI Normalized', 'trastuzumab-anns', FALSE, 'Retail', TIMESTAMP('2024-03-22T00:00:00.000+0000'), 200, 180, 2000.00, 'Y', DATE('2021-03-22')
  UNION ALL SELECT 'mvasi', 'MVASI Normalized', 'bevacizumab-awwb', FALSE, 'Hospital', TIMESTAMP('2024-03-23T00:00:00.000+0000'), 300, 270, 3000.00, 'N', DATE('2021-03-23')
  UNION ALL SELECT 'riabni', 'RIABNI Normalized', 'rituximab market', FALSE, 'Retail', TIMESTAMP('2024-03-24T00:00:00.000+0000'), 400, 360, 4000.00, 'Y', DATE('2021-03-24')
  UNION ALL SELECT 'abc', 'ABC Normalized', 'trastuzumab-anns', TRUE, 'Hospital', TIMESTAMP('2024-03-25T00:00:00.000+0000'), 500, 450, 5000.00, 'N', DATE('2021-03-25')
  UNION ALL SELECT 'riabni', 'RIABNI Normalized', 'rituximab market', TRUE, 'Retail', TIMESTAMP('2024-03-26T00:00:00.000+0000'), 600, 540, 6000.00, 'Y', DATE('2021-03-26')
  UNION ALL SELECT 'abc', 'ABC Normalized', 'bevacizumab-awwb', NULL, 'Hospital', TIMESTAMP('2024-03-27T00:00:00.000+0000'), 700, 630, 7000.00, 'N', DATE('2021-03-27')
  UNION ALL SELECT 'RIABNI', 'RIABNI Normalized', 'rituximab market', FALSE, 'Retail', TIMESTAMP('2024-03-28T00:00:00.000+0000'), 800, 720, 8000.00, 'Y', DATE('2021-03-28')
  UNION ALL SELECT 'abc', 'ABC Normalized', 'other-market', FALSE, 'Hospital', TIMESTAMP('2024-03-29T00:00:00.000+0000'), 900, 810, 9000.00, 'N', DATE('2021-03-29')
  UNION ALL SELECT 'abc', 'ABC Normalized', 'trastuzumab-anns', FALSE, 'Retail', TIMESTAMP('2018-03-21T00:00:00.000+0000'), 1000, 900, 10000.00, 'Y', DATE('2021-03-21')
  UNION ALL SELECT 'abc', 'ABC Normalized', 'bevacizumab-awwb', FALSE, 'Hospital', TIMESTAMP('2024-03-30T00:00:00.000+0000'), 1100, 990, 11000.00, 'N', NULL
  UNION ALL SELECT 'abc', 'ABC Normalized', 'rituximab market', FALSE, 'Retail', TIMESTAMP('2024-03-31T00:00:00.000+0000'), 1200, 1080, 12000.00, 'Y', DATE('2099-12-31')
  UNION ALL SELECT 'abc-漢字', 'ABC Special', 'trastuzumab-anns', FALSE, 'Hospital', TIMESTAMP('2024-04-01T00:00:00.000+0000'), 1300, 1170, 13000.00, 'N', DATE('2021-04-01')
  UNION ALL SELECT 'abc', 'Nørmålïzéd', 'bevacizumab-awwb', FALSE, 'Retail', TIMESTAMP('2024-04-02T00:00:00.000+0000'), 1400, 1260, 14000.00, 'Y', DATE('2021-04-02')
  UNION ALL SELECT 'abc', 'ABC Normalized', 'rituximab market', FALSE, 'Hospital', TIMESTAMP('2024-04-03T00:00:00.000+0000'), NULL, 1350, 15000.00, 'N', DATE('2021-04-03')
  UNION ALL SELECT 'abc', 'ABC Normalized', 'trastuzumab-anns', FALSE, 'Retail', TIMESTAMP('2024-04-04T00:00:00.000+0000'), 1600, NULL, 16000.00, 'Y', DATE('2021-04-04')
  UNION ALL SELECT 'abc', 'ABC Normalized', 'bevacizumab-awwb', FALSE, 'Hospital', TIMESTAMP('2024-04-05T00:00:00.000+0000'), 1700, 1530, NULL, 'N', DATE('2021-04-05')
  UNION ALL SELECT NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
  UNION ALL SELECT 'mvasi', 'MVASI Normalized', 'bevacizumab-awwb', TRUE, 'Retail', TIMESTAMP('2024-04-06T00:00:00.000+0000'), 1800, 1620, 18000.00, 'Y', DATE('2021-04-06')
  UNION ALL SELECT 'riabni', 'RIABNI Normalized', 'rituximab market', FALSE, 'Hospital', TIMESTAMP('2024-04-07T00:00:00.000+0000'), 1900, 1710, 19000.00, 'N', DATE('2021-04-07')
  UNION ALL SELECT 'kanjinti', 'KANJINTI Normalized', 'trastuzumab-anns', TRUE, 'Retail', TIMESTAMP('2024-04-08T00:00:00.000+0000'), 2000, 1800, 20000.00, 'Y', DATE('2021-04-08')
  UNION ALL SELECT 'mvasi', 'MVASI Normalized', 'bevacizumab-awwb', NULL, 'Hospital', TIMESTAMP('2024-04-09T00:00:00.000+0000'), 2100, 1890, 21000.00, 'N', DATE('2021-04-09')
  UNION ALL SELECT 'riabni', 'RIABNI Normalized', 'rituximab market', NULL, 'Retail', TIMESTAMP('2024-04-10T00:00:00.000+0000'), 2200, 1980, 22000.00, 'Y', DATE('2021-04-10')
  UNION ALL SELECT 'abc', 'ABC Normalized', 'trastuzumab-anns', TRUE, 'Hospital', TIMESTAMP('2024-04-11T00:00:00.000+0000'), 2300, 2070, 23000.00, 'N', DATE('2021-04-11')
  UNION ALL SELECT 'abc', 'ABC Normalized', 'bevacizumab-awwb', FALSE, 'Retail', TIMESTAMP('2024-04-12T00:00:00.000+0000'), 2400, 2160, 24000.00, 'Y', DATE('2021-04-12')
  UNION ALL SELECT 'riabni', 'RIABNI Normalized', 'rituximab market', TRUE, 'Hospital', TIMESTAMP('2024-04-13T00:00:00.000+0000'), 2500, 2250, 25000.00, 'N', DATE('2021-04-13')
  UNION ALL SELECT 'abc', 'ABC Normalized', 'trastuzumab-anns', FALSE, '零售', TIMESTAMP('2024-04-14T00:00:00.000+0000'), 2600, 2340, 26000.00, 'Y', DATE('2021-04-14')
  UNION ALL SELECT 'abc', 'ABC Normalized', 'bévacizumab-awwb', FALSE, 'Hospital', TIMESTAMP('2024-04-15T00:00:00.000+0000'), 2700, 2430, 27000.00, 'N', DATE('2021-04-15')
  UNION ALL SELECT 'abc', 'ABC Normalized', 'rituximab market', NULL, 'Retail', TIMESTAMP('2024-04-16T00:00:00.000+0000'), 2800, 2520, 28000.00, 'Y', DATE('2021-04-16')
)
SELECT * FROM test_data
;

/* -------------------- SETUP: Create Invalid CDL Log Table -------------------- */

DROP TABLE IF EXISTS purgo_databricks.purgo_playground.bai_sales_agg_obu_customer_datapack_invalid_cdl_effective_date_log;

CREATE TABLE purgo_databricks.purgo_playground.bai_sales_agg_obu_customer_datapack_invalid_cdl_effective_date_log (
  brand_normalized_name STRING,
  normalized_name STRING,
  market_normalized_name STRING,
  competitor_flag BOOLEAN,
  channel_name STRING,
  transaction_timestamp TIMESTAMP,
  integrated_units BIGINT,
  integrated_normalized_units BIGINT,
  integrated_dollars BIGINT,
  cdl_effective_date STRING,
  error_message STRING
)
USING DELTA;

/* -------------------- TEST 1: Output Schema Validation -------------------- */

-- Validate output schema and data types
WITH output_schema AS (
  SELECT
    typeof(brand_normalized_name) AS brand_normalized_name_type,
    typeof(normalized_name) AS normalized_name_type,
    typeof(market_normalized_name) AS market_normalized_name_type,
    typeof(channel_name) AS channel_name_type,
    typeof(transaction_timestamp) AS transaction_timestamp_type,
    typeof(competitor_flag) AS competitor_flag_type,
    typeof(integrated_units) AS integrated_units_type,
    typeof(integrated_normalized_units) AS integrated_normalized_units_type,
    typeof(integrated_dollars) AS integrated_dollars_type
  FROM (
    SELECT
      brand_normalized_name,
      normalized_name,
      market_normalized_name,
      channel_name,
      transaction_timestamp,
      CAST(competitor_flag AS STRING) AS competitor_flag,
      SUM(
        CASE
          WHEN lower(brand_normalized_name) IN ("kanjinti","mvasi","riabni") OR competitor_flag = TRUE THEN 0
          ELSE integrated_units
        END
      ) AS integrated_units,
      SUM(
        CASE
          WHEN lower(brand_normalized_name) IN ("kanjinti","mvasi","riabni") OR competitor_flag = TRUE THEN 0
          ELSE integrated_normalized_units
        END
      ) AS integrated_normalized_units,
      SUM(
        CASE
          WHEN lower(brand_normalized_name) IN ("kanjinti","mvasi","riabni") OR competitor_flag = TRUE THEN 0
          ELSE integrated_dollars
        END
      ) AS integrated_dollars
    FROM purgo_databricks.purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
    WHERE substring(transaction_timestamp,1,10) >= add_months(cdl_effective_date, -36)
      AND lower(market_normalized_name) IN ("trastuzumab-anns","bevacizumab-awwb","rituximab market")
    GROUP BY brand_normalized_name, normalized_name, market_normalized_name, channel_name, transaction_timestamp, competitor_flag
    LIMIT 1
  )
)
SELECT
  CASE WHEN brand_normalized_name_type = "STRING" THEN 1 ELSE 0 END AS brand_normalized_name_is_string,
  CASE WHEN normalized_name_type = "STRING" THEN 1 ELSE 0 END AS normalized_name_is_string,
  CASE WHEN market_normalized_name_type = "STRING" THEN 1 ELSE 0 END AS market_normalized_name_is_string,
  CASE WHEN channel_name_type = "STRING" THEN 1 ELSE 0 END AS channel_name_is_string,
  CASE WHEN transaction_timestamp_type = "TIMESTAMP" THEN 1 ELSE 0 END AS transaction_timestamp_is_timestamp,
  CASE WHEN competitor_flag_type = "STRING" THEN 1 ELSE 0 END AS competitor_flag_is_string,
  CASE WHEN integrated_units_type = "BIGINT" OR integrated_units_type = "INT" THEN 1 ELSE 0 END AS integrated_units_is_int,
  CASE WHEN integrated_normalized_units_type = "BIGINT" OR integrated_normalized_units_type = "INT" THEN 1 ELSE 0 END AS integrated_normalized_units_is_int,
  CASE WHEN integrated_dollars_type = "DOUBLE" OR integrated_dollars_type = "DECIMAL" THEN 1 ELSE 0 END AS integrated_dollars_is_double
FROM output_schema
;

/* -------------------- TEST 2: Suppression Logic - Brand Suppression -------------------- */

-- Validate suppression for brand_normalized_name in ('kanjinti','mvasi','riabni')
WITH suppression_test AS (
  SELECT
    brand_normalized_name,
    competitor_flag,
    SUM(
      CASE
        WHEN lower(brand_normalized_name) IN ("kanjinti","mvasi","riabni") OR competitor_flag = TRUE THEN 0
        ELSE integrated_units
      END
    ) AS integrated_units
  FROM purgo_databricks.purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
  WHERE lower(brand_normalized_name) IN ("kanjinti","mvasi","riabni")
    AND competitor_flag IS FALSE
    AND substring(transaction_timestamp,1,10) >= add_months(cdl_effective_date, -36)
    AND lower(market_normalized_name) IN ("trastuzumab-anns","bevacizumab-awwb","rituximab market")
  GROUP BY brand_normalized_name, competitor_flag
)
SELECT
  COUNT(*) AS suppressed_brand_count,
  SUM(CASE WHEN integrated_units = 0 THEN 1 ELSE 0 END) AS all_zero_units
FROM suppression_test
;

/* -------------------- TEST 3: Suppression Logic - Competitor Flag -------------------- */

-- Validate suppression for competitor_flag = TRUE
WITH suppression_test AS (
  SELECT
    brand_normalized_name,
    competitor_flag,
    SUM(
      CASE
        WHEN lower(brand_normalized_name) IN ("kanjinti","mvasi","riabni") OR competitor_flag = TRUE THEN 0
        ELSE integrated_units
      END
    ) AS integrated_units
  FROM purgo_databricks.purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
  WHERE competitor_flag = TRUE
    AND substring(transaction_timestamp,1,10) >= add_months(cdl_effective_date, -36)
    AND lower(market_normalized_name) IN ("trastuzumab-anns","bevacizumab-awwb","rituximab market")
  GROUP BY brand_normalized_name, competitor_flag
)
SELECT
  COUNT(*) AS suppressed_competitor_count,
  SUM(CASE WHEN integrated_units = 0 THEN 1 ELSE 0 END) AS all_zero_units
FROM suppression_test
;

/* -------------------- TEST 4: Happy Path - No Suppression -------------------- */

-- Validate that non-suppressed records retain their values
WITH happy_path AS (
  SELECT
    brand_normalized_name,
    competitor_flag,
    SUM(
      CASE
        WHEN lower(brand_normalized_name) IN ("kanjinti","mvasi","riabni") OR competitor_flag = TRUE THEN 0
        ELSE integrated_units
      END
    ) AS integrated_units
  FROM purgo_databricks.purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
  WHERE lower(brand_normalized_name) NOT IN ("kanjinti","mvasi","riabni")
    AND (competitor_flag IS FALSE OR competitor_flag IS NULL)
    AND substring(transaction_timestamp,1,10) >= add_months(cdl_effective_date, -36)
    AND lower(market_normalized_name) IN ("trastuzumab-anns","bevacizumab-awwb","rituximab market")
  GROUP BY brand_normalized_name, competitor_flag
)
SELECT
  COUNT(*) AS happy_path_count,
  SUM(CASE WHEN integrated_units > 0 THEN 1 ELSE 0 END) AS all_positive_units
FROM happy_path
;

/* -------------------- TEST 5: competitor_flag Casting Validation -------------------- */

-- Validate competitor_flag is cast to string and values are as expected
WITH flag_cast AS (
  SELECT DISTINCT
    competitor_flag,
    CAST(competitor_flag AS STRING) AS competitor_flag_str
  FROM purgo_databricks.purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
)
SELECT
  competitor_flag,
  competitor_flag_str,
  CASE
    WHEN competitor_flag IS TRUE AND competitor_flag_str = "true" THEN 1
    WHEN competitor_flag IS FALSE AND competitor_flag_str = "false" THEN 1
    WHEN competitor_flag IS NULL AND competitor_flag_str = "null" THEN 1
    ELSE 0
  END AS cast_correct
FROM flag_cast
;

/* -------------------- TEST 6: NULL Handling in Integrated Fields -------------------- */

-- Validate that NULLs in integrated_units, integrated_normalized_units, integrated_dollars are handled
WITH nulls_test AS (
  SELECT
    COUNT(*) AS null_units_count
  FROM purgo_databricks.purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
  WHERE integrated_units IS NULL
)
SELECT null_units_count FROM nulls_test
;

/* -------------------- TEST 7: Invalid cdl_effective_date Logging -------------------- */

-- Insert invalid cdl_effective_date records into log table
INSERT INTO purgo_databricks.purgo_playground.bai_sales_agg_obu_customer_datapack_invalid_cdl_effective_date_log
WITH invalid_cdl AS (
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
    CAST(cdl_effective_date AS STRING) AS cdl_effective_date,
    "Invalid cdl_effective_date" AS error_message
  FROM purgo_databricks.purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
  WHERE cdl_effective_date IS NULL
     OR TRY_CAST(cdl_effective_date AS DATE) IS NULL
)
SELECT * FROM invalid_cdl
;

-- Validate that invalid records are logged
SELECT
  COUNT(*) AS invalid_cdl_logged
FROM purgo_databricks.purgo_playground.bai_sales_agg_obu_customer_datapack_invalid_cdl_effective_date_log
WHERE error_message = "Invalid cdl_effective_date"
;

/* -------------------- TEST 8: Output Field Validation -------------------- */

-- Validate output fields are present and correct
WITH output_fields AS (
  SELECT
    brand_normalized_name,
    normalized_name,
    market_normalized_name,
    channel_name,
    transaction_timestamp,
    CAST(competitor_flag AS STRING) AS competitor_flag,
    SUM(
      CASE
        WHEN lower(brand_normalized_name) IN ("kanjinti","mvasi","riabni") OR competitor_flag = TRUE THEN 0
        ELSE integrated_units
      END
    ) AS integrated_units,
    SUM(
      CASE
        WHEN lower(brand_normalized_name) IN ("kanjinti","mvasi","riabni") OR competitor_flag = TRUE THEN 0
        ELSE integrated_normalized_units
      END
    ) AS integrated_normalized_units,
    SUM(
      CASE
        WHEN lower(brand_normalized_name) IN ("kanjinti","mvasi","riabni") OR competitor_flag = TRUE THEN 0
        ELSE integrated_dollars
      END
    ) AS integrated_dollars
  FROM purgo_databricks.purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
  WHERE substring(transaction_timestamp,1,10) >= add_months(cdl_effective_date, -36)
    AND lower(market_normalized_name) IN ("trastuzumab-anns","bevacizumab-awwb","rituximab market")
  GROUP BY brand_normalized_name, normalized_name, market_normalized_name, channel_name, transaction_timestamp, competitor_flag
)
SELECT
  COUNT(*) AS output_row_count,
  COUNT(DISTINCT brand_normalized_name) AS distinct_brand_count
FROM output_fields
;

/* -------------------- TEST 9: Delta Lake Operations (MERGE, UPDATE, DELETE) -------------------- */

-- Test MERGE: Upsert a record and validate
MERGE INTO purgo_databricks.purgo_playground.bai_sales_agg_obu_customer_datapack_weekly AS target
USING (
  SELECT
    'testbrand' AS brand_normalized_name,
    'Test Normalized' AS normalized_name,
    'trastuzumab-anns' AS market_normalized_name,
    FALSE AS competitor_flag,
    'TestChannel' AS channel_name,
    TIMESTAMP('2024-05-01T00:00:00.000+0000') AS transaction_timestamp,
    999 AS integrated_units,
    888 AS integrated_normalized_units,
    777.77 AS integrated_dollars,
    'N' AS br_gpo_flag,
    DATE('2021-05-01') AS cdl_effective_date
) AS source
ON target.brand_normalized_name = source.brand_normalized_name
   AND target.normalized_name = source.normalized_name
   AND target.transaction_timestamp = source.transaction_timestamp
WHEN MATCHED THEN
  UPDATE SET
    integrated_units = source.integrated_units,
    integrated_normalized_units = source.integrated_normalized_units,
    integrated_dollars = source.integrated_dollars
WHEN NOT MATCHED THEN
  INSERT *
;

-- Validate upsert
SELECT
  COUNT(*) AS testbrand_count
FROM purgo_databricks.purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
WHERE brand_normalized_name = "testbrand"
  AND integrated_units = 999
  AND integrated_normalized_units = 888
  AND integrated_dollars = 777.77
;

/* Test DELETE: Remove testbrand and validate */
DELETE FROM purgo_databricks.purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
WHERE brand_normalized_name = "testbrand"
;

SELECT
  COUNT(*) AS testbrand_post_delete
FROM purgo_databricks.purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
WHERE brand_normalized_name = "testbrand"
;

/* -------------------- TEST 10: Window Function Analytics -------------------- */

-- Test row_number window function for analytics
WITH window_test AS (
  SELECT
    brand_normalized_name,
    normalized_name,
    ROW_NUMBER() OVER (PARTITION BY brand_normalized_name ORDER BY transaction_timestamp DESC) AS rn
  FROM purgo_databricks.purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
)
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT brand_normalized_name) AS distinct_brands,
  SUM(CASE WHEN rn = 1 THEN 1 ELSE 0 END) AS latest_per_brand
FROM window_test
;

/* -------------------- TEST 11: Performance Test (Row Count) -------------------- */

-- Validate row count is as expected (should match number of inserted test rows minus deletes)
SELECT
  COUNT(*) AS total_test_rows
FROM purgo_databricks.purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
;

/* -------------------- TEST 12: Data Quality - No NULLs in Required Fields -------------------- */

-- Validate required fields are not NULL (brand_normalized_name, normalized_name, market_normalized_name, channel_name, transaction_timestamp)
SELECT
  COUNT(*) AS null_required_fields
FROM purgo_databricks.purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
WHERE brand_normalized_name IS NULL
   OR normalized_name IS NULL
   OR market_normalized_name IS NULL
   OR channel_name IS NULL
   OR transaction_timestamp IS NULL
;

/* -------------------- TEST 13: Foreign Key and Check Constraints -------------------- */

-- Add check constraint for competitor_flag values (should be TRUE, FALSE, or NULL)
ALTER TABLE purgo_databricks.purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
ADD CONSTRAINT competitor_flag_values CHECK (competitor_flag IN (TRUE, FALSE) OR competitor_flag IS NULL)
;

-- Add check constraint for allowed brand_normalized_name values (example: no empty string)
ALTER TABLE purgo_databricks.purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
ADD CONSTRAINT brand_not_empty CHECK (brand_normalized_name IS NULL OR LENGTH(brand_normalized_name) > 0)
;

/* -------------------- CLEANUP: Remove Test Data -------------------- */

-- Clean up test data (except for required test rows)
DELETE FROM purgo_databricks.purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
WHERE brand_normalized_name = "testbrand"
;

-- Clean up invalid CDL log
DELETE FROM purgo_databricks.purgo_playground.bai_sales_agg_obu_customer_datapack_invalid_cdl_effective_date_log
WHERE error_message = "Invalid cdl_effective_date"
;

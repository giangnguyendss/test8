/* 
==========================================================================================
Databricks SQL Implementation: Aggregated Sales Calculation with Business Rules
==========================================================================================

- Source Table: purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
- Aggregation by: brand_normalized_name, normalized_name, market_normalized_name, competitor_flag, channel_name, transaction_timestamp
- Aggregated Columns: SUM of integrated_units, integrated_normalized_units, integrated_dollars
- Business Rules:
    - If brand_normalized_name IN ('kanjinti', 'mvasi', 'riabni') AND br_gpo_flag LIKE '%KAISER%', set sales columns to zero before aggregation
    - Only include rows where market_normalized_name IN ('trastuzumab-anns', 'bevacizumab-awwb', 'rituximab market')
    - Only include rows where transaction_timestamp is within last 36 months from cdl_effective_date (transaction_timestamp >= DATE_ADD(cdl_effective_date, -36*30) AND transaction_timestamp <= cdl_effective_date)
    - Treat NULLs in sales columns as zero in aggregation
    - Exclude rows with NULL in any required field
    - Log errors for invalid cdl_effective_date (NULL) and missing required fields

==========================================================================================
*/

/* ---------------------------
   Setup: Use correct catalog
---------------------------- */
USE CATALOG purgo_databricks;

/* -----------------------------------------------------------
   Section: Error Logging for Invalid cdl_effective_date Format
------------------------------------------------------------ */
-- Log rows with invalid cdl_effective_date (NULL)
INSERT INTO purgo_playground.bai_sales_agg_obu_customer_datapack_invalid_cdl_effective_date_log
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
  br_gpo_flag,
  CAST(cdl_effective_date AS STRING) AS cdl_effective_date,
  'Invalid cdl_effective_date format' AS error_message
FROM purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
WHERE cdl_effective_date IS NULL;

/* -----------------------------------------------------------
   Section: Error Logging for Missing Required Fields
------------------------------------------------------------ */
-- Log rows with missing required fields
INSERT INTO purgo_playground.bai_sales_agg_obu_customer_datapack_invalid_cdl_effective_date_log
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
  br_gpo_flag,
  CAST(cdl_effective_date AS STRING) AS cdl_effective_date,
  CASE
    WHEN brand_normalized_name IS NULL THEN 'Missing required field: brand_normalized_name'
    WHEN normalized_name IS NULL THEN 'Missing required field: normalized_name'
    WHEN market_normalized_name IS NULL THEN 'Missing required field: market_normalized_name'
    WHEN competitor_flag IS NULL THEN 'Missing required field: competitor_flag'
    WHEN channel_name IS NULL THEN 'Missing required field: channel_name'
    WHEN transaction_timestamp IS NULL THEN 'Missing required field: transaction_timestamp'
    WHEN cdl_effective_date IS NULL THEN 'Missing required field: cdl_effective_date'
    ELSE NULL
  END AS error_message
FROM purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
WHERE
  brand_normalized_name IS NULL
  OR normalized_name IS NULL
  OR market_normalized_name IS NULL
  OR competitor_flag IS NULL
  OR channel_name IS NULL
  OR transaction_timestamp IS NULL
  OR cdl_effective_date IS NULL;

/* -----------------------------------------------------------
   Section: Aggregation Query with Business Rules
------------------------------------------------------------ */
-- CTE: valid_sales_rows
WITH valid_sales_rows AS (
  SELECT
    brand_normalized_name AS brand_normalized_name,
    normalized_name AS normalized_name,
    market_normalized_name AS market_normalized_name,
    competitor_flag AS competitor_flag,
    channel_name AS channel_name,
    transaction_timestamp AS transaction_timestamp,
    -- Apply KAISER zeroing logic and NULL handling for integrated_units
    CASE
      WHEN brand_normalized_name IN ('kanjinti', 'mvasi', 'riabni')
        AND br_gpo_flag LIKE '%KAISER%'
        THEN 0
      ELSE COALESCE(integrated_units, 0)
    END AS integrated_units,
    -- Apply KAISER zeroing logic and NULL handling for integrated_normalized_units
    CASE
      WHEN brand_normalized_name IN ('kanjinti', 'mvasi', 'riabni')
        AND br_gpo_flag LIKE '%KAISER%'
        THEN 0
      ELSE COALESCE(integrated_normalized_units, 0)
    END AS integrated_normalized_units,
    -- Apply KAISER zeroing logic and NULL handling for integrated_dollars
    CASE
      WHEN brand_normalized_name IN ('kanjinti', 'mvasi', 'riabni')
        AND br_gpo_flag LIKE '%KAISER%'
        THEN 0.0
      ELSE COALESCE(integrated_dollars, 0.0)
    END AS integrated_dollars
  FROM purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
  WHERE
    -- Only allowed markets
    market_normalized_name IN ('trastuzumab-anns', 'bevacizumab-awwb', 'rituximab market')
    -- Required fields must not be NULL
    AND brand_normalized_name IS NOT NULL
    AND normalized_name IS NOT NULL
    AND market_normalized_name IS NOT NULL
    AND competitor_flag IS NOT NULL
    AND channel_name IS NOT NULL
    AND transaction_timestamp IS NOT NULL
    AND cdl_effective_date IS NOT NULL
    -- Transaction date within last 36 months from cdl_effective_date
    AND transaction_timestamp >= DATE_ADD(cdl_effective_date, -36*30)
    AND transaction_timestamp <= cdl_effective_date
)

/* -----------------------------------------------------------
   Section: Final Aggregation Output
------------------------------------------------------------ */
SELECT
  brand_normalized_name,
  normalized_name,
  market_normalized_name,
  competitor_flag,
  channel_name,
  transaction_timestamp,
  SUM(integrated_units) AS total_integrated_units,
  SUM(integrated_normalized_units) AS total_integrated_normalized_units,
  SUM(integrated_dollars) AS total_integrated_dollars
FROM valid_sales_rows
GROUP BY
  brand_normalized_name,
  normalized_name,
  market_normalized_name,
  competitor_flag,
  channel_name,
  transaction_timestamp
ORDER BY
  brand_normalized_name,
  normalized_name,
  market_normalized_name,
  competitor_flag,
  channel_name,
  transaction_timestamp
;
-- End of implementation code


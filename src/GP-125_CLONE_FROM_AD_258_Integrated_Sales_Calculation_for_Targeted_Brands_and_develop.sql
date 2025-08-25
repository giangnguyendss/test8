/*
  Databricks SQL Script: Integrated Sales Aggregation by Brand, Product, Market, Competitor Flag, Channel, and Transaction Time

  Catalog: purgo_databricks
  Schema: purgo_playground

  Requirements:
    - Aggregate SUM of integrated_units, integrated_normalized_units, integrated_dollars
    - Group by brand_normalized_name, normalized_name, market_normalized_name, competitor_flag, channel_name, transaction_timestamp
    - For brands 'kanjinti', 'mvasi', 'riabni' with br_gpo_flag containing 'KAISER' (case-insensitive), treat sales as zero
    - Only include rows where transaction_timestamp is within 36 months prior to cdl_effective_date (inclusive)
    - Only include markets: 'trastuzumab-anns', 'bevacizumab-awwb', 'rituximab market'
    - Exclude rows with NULL or invalid aggregation keys or cdl_effective_date
    - Data type validation and conversion for aggregation columns
    - Error logging for invalid cdl_effective_date and missing aggregation keys
*/

/* Use correct catalog and schema */
USE CATALOG purgo_databricks;

/* 
  CTE: error_log
  Logs rows with invalid cdl_effective_date or missing aggregation keys
*/
WITH error_log AS (
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
    cdl_effective_date,
    CASE
      WHEN cdl_effective_date IS NULL THEN "Invalid cdl_effective_date"
      WHEN brand_normalized_name IS NULL OR normalized_name IS NULL OR market_normalized_name IS NULL OR competitor_flag IS NULL OR channel_name IS NULL OR transaction_timestamp IS NULL THEN "Missing aggregation key"
      ELSE NULL
    END AS error_message
  FROM purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
  WHERE
    cdl_effective_date IS NULL
    OR brand_normalized_name IS NULL
    OR normalized_name IS NULL
    OR market_normalized_name IS NULL
    OR competitor_flag IS NULL
    OR channel_name IS NULL
    OR transaction_timestamp IS NULL
),

/*
  CTE: valid_sales
  Filters and transforms valid rows for aggregation
*/
valid_sales AS (
  SELECT
    brand_normalized_name,
    normalized_name,
    market_normalized_name,
    competitor_flag,
    channel_name,
    transaction_timestamp,
    /* Zero out sales for specified brands with KAISER in GPO flag */
    CAST(
      CASE
        WHEN brand_normalized_name IN ("kanjinti","mvasi","riabni")
          AND br_gpo_flag IS NOT NULL
          AND LOWER(br_gpo_flag) LIKE "%kaiser%"
        THEN 0
        ELSE integrated_units
      END AS INT
    ) AS integrated_units,
    CAST(
      CASE
        WHEN brand_normalized_name IN ("kanjinti","mvasi","riabni")
          AND br_gpo_flag IS NOT NULL
          AND LOWER(br_gpo_flag) LIKE "%kaiser%"
        THEN 0
        ELSE integrated_normalized_units
      END AS INT
    ) AS integrated_normalized_units,
    CAST(
      CASE
        WHEN brand_normalized_name IN ("kanjinti","mvasi","riabni")
          AND br_gpo_flag IS NOT NULL
          AND LOWER(br_gpo_flag) LIKE "%kaiser%"
        THEN 0.0
        ELSE integrated_dollars
      END AS DOUBLE
    ) AS integrated_dollars
  FROM purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
  WHERE
    /* Only allowed markets */
    market_normalized_name IN ("trastuzumab-anns","bevacizumab-awwb","rituximab market")
    /* Only valid cdl_effective_date */
    AND cdl_effective_date IS NOT NULL
    /* Only valid aggregation keys */
    AND brand_normalized_name IS NOT NULL
    AND normalized_name IS NOT NULL
    AND market_normalized_name IS NOT NULL
    AND competitor_flag IS NOT NULL
    AND channel_name IS NOT NULL
    AND transaction_timestamp IS NOT NULL
    /* Only transaction_timestamp within 36 months window (inclusive) */
    AND transaction_timestamp >= DATE_ADD(cdl_effective_date, -1080)
    AND transaction_timestamp <= cdl_effective_date
)

/*
  Final Aggregation Query
  - Aggregates sales by required keys
  - Ensures output columns match schema and types
*/
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
FROM valid_sales
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

/*
  Error Log Output
  - For monitoring and data quality checks
*/
SELECT * FROM error_log
WHERE error_message IS NOT NULL
ORDER BY transaction_timestamp
;


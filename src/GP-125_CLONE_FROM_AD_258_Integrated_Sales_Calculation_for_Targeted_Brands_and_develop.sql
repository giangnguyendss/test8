USE CATALOG purgo_databricks;

/*
  Integrated Sales Aggregation Script
  Catalog: purgo_databricks
  Schema: purgo_playground
  Source Table: bai_sales_agg_obu_customer_datapack_weekly

  Business Logic:
    - Aggregate sales for each combination of brand_name, product_name, market_name, competitor_flag, channel, transaction_time
    - Sum integrated_units, integrated_normalized_units, integrated_dollars
    - If brand_name is "kanjinti", "mvasi", or "riabni" (case-sensitive) AND gpo_flag contains "KAISER" (case-insensitive), set sales values to zero
    - Only include rows where transaction_time is within 36 months prior to and including cdl_effective_date
    - Only include market_name in ("trastuzumab-anns", "bevacizumab-awwb", "rituximab market")
    - Exclude rows with NULL in any required field
    - Output columns in required order
*/

/*------------------*/
/* Aggregation Query*/
/*------------------*/

WITH filtered_sales AS (
  SELECT
    brand_name AS brand_name,
    product_name AS product_name,
    market_name AS market_name,
    competitor_flag AS competitor_flag,
    channel AS channel,
    transaction_time AS transaction_time,
    gpo_flag AS gpo_flag,
    cdl_effective_date AS cdl_effective_date,
    integrated_units AS integrated_units,
    integrated_normalized_units AS integrated_normalized_units,
    integrated_dollars AS integrated_dollars
  FROM purgo_databricks.purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
  WHERE
    market_name IN ('trastuzumab-anns', 'bevacizumab-awwb', 'rituximab market')
    AND transaction_time IS NOT NULL
    AND cdl_effective_date IS NOT NULL
    AND transaction_time >= ADD_MONTHS(cdl_effective_date, -36)
    AND transaction_time <= cdl_effective_date
    AND brand_name IS NOT NULL
    AND product_name IS NOT NULL
    AND competitor_flag IS NOT NULL
    AND channel IS NOT NULL
    AND gpo_flag IS NOT NULL
    AND integrated_units IS NOT NULL
    AND integrated_normalized_units IS NOT NULL
    AND integrated_dollars IS NOT NULL
)
SELECT
  brand_name,
  product_name,
  market_name,
  competitor_flag,
  channel,
  transaction_time,
  -- Zero sales if brand in ('kanjinti','mvasi','riabni') and gpo_flag contains 'KAISER' (case-insensitive)
  CASE
    WHEN brand_name IN ('kanjinti', 'mvasi', 'riabni')
      AND LOWER(gpo_flag) LIKE '%kaiser%'
    THEN 0.00
    ELSE SUM(integrated_units)
  END AS integrated_units,
  CASE
    WHEN brand_name IN ('kanjinti', 'mvasi', 'riabni')
      AND LOWER(gpo_flag) LIKE '%kaiser%'
    THEN 0.00
    ELSE SUM(integrated_normalized_units)
  END AS integrated_normalized_units,
  CASE
    WHEN brand_name IN ('kanjinti', 'mvasi', 'riabni')
      AND LOWER(gpo_flag) LIKE '%kaiser%'
    THEN 0.00
    ELSE SUM(integrated_dollars)
  END AS integrated_dollars
FROM filtered_sales
GROUP BY
  brand_name,
  product_name,
  market_name,
  competitor_flag,
  channel,
  transaction_time
ORDER BY brand_name, product_name, market_name, competitor_flag, channel, transaction_time;
-- End of script

USE CATALOG purgo_databricks;

/*
  Databricks SQL Script: Integrated Sales Aggregation for Targeted Brands and Markets
  Catalog: purgo_databricks
  Schema: purgo_playground

  This script calculates the total integrated_units, integrated_normalized_units, and integrated_dollars
  for each unique combination of brand_normalized_name, normalized_name, market_normalized_name,
  competitor_flag, channel_name, and transaction_timestamp from purgo_playground.bai_sales_agg_obu_customer_datapack_weekly,
  applying the following business rules:
    - If brand_normalized_name is "kanjinti", "mvasi", or "riabni" AND br_gpo_flag contains "KAISER",
      treat integrated_units, integrated_normalized_units, and integrated_dollars as zero for those rows.
    - Only include rows where market_normalized_name is one of ("trastuzumab-anns", "bevacizumab-awwb", "rituximab market").
    - Only include rows where transaction_timestamp is within the last 36 months from cdl_effective_date.
    - Exclude rows with NULLs in any key columns.
    - Treat NULLs in aggregation columns as zero.
    - Exclude rows with invalid cdl_effective_date (NULL after cast).
    - Output columns: brand_normalized_name, normalized_name, market_normalized_name, competitor_flag, channel_name, transaction_timestamp,
      total_integrated_units, total_integrated_normalized_units, total_integrated_dollars.
*/

/* ------------------ Aggregation Query ------------------ */
WITH filtered_sales AS (
  SELECT
    brand_normalized_name,
    normalized_name,
    market_normalized_name,
    competitor_flag,
    channel_name,
    transaction_timestamp,
    -- Business rule: zero out sales for targeted brands with KAISER GPO flag
    CASE
      WHEN brand_normalized_name IN ('kanjinti', 'mvasi', 'riabni')
        AND br_gpo_flag LIKE '%KAISER%'
        THEN 0
      ELSE COALESCE(integrated_units, 0)
    END AS integrated_units,
    CASE
      WHEN brand_normalized_name IN ('kanjinti', 'mvasi', 'riabni')
        AND br_gpo_flag LIKE '%KAISER%'
        THEN 0
      ELSE COALESCE(integrated_normalized_units, 0)
    END AS integrated_normalized_units,
    CASE
      WHEN brand_normalized_name IN ('kanjinti', 'mvasi', 'riabni')
        AND br_gpo_flag LIKE '%KAISER%'
        THEN 0.0
      ELSE COALESCE(integrated_dollars, 0.0)
    END AS integrated_dollars,
    cdl_effective_date
  FROM purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
  WHERE
    brand_normalized_name IS NOT NULL
    AND normalized_name IS NOT NULL
    AND market_normalized_name IS NOT NULL
    AND competitor_flag IS NOT NULL
    AND channel_name IS NOT NULL
    AND transaction_timestamp IS NOT NULL
    AND cdl_effective_date IS NOT NULL
    AND market_normalized_name IN ('trastuzumab-anns', 'bevacizumab-awwb', 'rituximab market')
    AND transaction_timestamp >= DATEADD(month, -36, cdl_effective_date)
    AND transaction_timestamp <= cdl_effective_date
)
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
FROM filtered_sales
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

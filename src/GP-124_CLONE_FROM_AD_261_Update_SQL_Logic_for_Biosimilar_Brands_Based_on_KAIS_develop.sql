USE CATALOG purgo_databricks;

/*
  Production-ready Databricks SQL for bai_sales.sql update
  Catalog: purgo_databricks
  Schema: purgo_playground

  Implements:
    - Conditional logic for br_gpo_flag and biosimilar brands (Kanjinti, Mvasi, Riabni)
    - competitor_flag cast to STRING and included in SELECT/GROUP BY
    - NULL handling for br_gpo_flag (treated as empty string)
    - For biosimilar brands: if br_gpo_flag contains "KAISER" (case-insensitive), units set to zero
    - For non-biosimilar brands: units always original values
    - Excludes invalid market_normalized_name and transaction_timestamp before cdl_effective_date minus 36 months
    - Output columns: brand_normalized_name, normalized_name, market_normalized_name, competitor_flag (as string), channel_name, transaction_timestamp, integrated_units, integrated_normalized_units, integrated_dollars
*/

/* ---------- Section: Main Query ---------- */

WITH bai_sales_cte AS (
  SELECT
    brand_normalized_name,
    normalized_name,
    market_normalized_name,
    -- Cast competitor_flag to string, handle NULL
    CASE
      WHEN competitor_flag IS TRUE THEN 'true'
      WHEN competitor_flag IS FALSE THEN 'false'
      WHEN competitor_flag IS NULL THEN 'null'
      ELSE 'invalid'
    END AS competitor_flag,
    channel_name,
    transaction_timestamp,
    -- Units logic for biosimilar brands and br_gpo_flag
    CASE
      WHEN brand_normalized_name IS NULL THEN
        CAST(NULL AS INT)
      WHEN lower(brand_normalized_name) IN ('kanjinti','mvasi','riabni') THEN
        CASE
          WHEN COALESCE(br_gpo_flag, '') LIKE '%kaiser%' THEN 0
          ELSE COALESCE(integrated_units, 0)
        END
      ELSE COALESCE(integrated_units, 0)
    END AS integrated_units,
    CASE
      WHEN brand_normalized_name IS NULL THEN
        CAST(NULL AS INT)
      WHEN lower(brand_normalized_name) IN ('kanjinti','mvasi','riabni') THEN
        CASE
          WHEN COALESCE(br_gpo_flag, '') LIKE '%kaiser%' THEN 0
          ELSE COALESCE(integrated_normalized_units, 0)
        END
      ELSE COALESCE(integrated_normalized_units, 0)
    END AS integrated_normalized_units,
    CASE
      WHEN brand_normalized_name IS NULL THEN
        CAST(NULL AS DOUBLE)
      WHEN lower(brand_normalized_name) IN ('kanjinti','mvasi','riabni') THEN
        CASE
          WHEN COALESCE(br_gpo_flag, '') LIKE '%kaiser%' THEN 0.0
          ELSE COALESCE(integrated_dollars, 0.0)
        END
      ELSE COALESCE(integrated_dollars, 0.0)
    END AS integrated_dollars
  FROM purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
  WHERE
    -- Exclude invalid market_normalized_name
    lower(market_normalized_name) IN ('trastuzumab-anns','bevacizumab-awwb','rituximab market')
    -- Exclude transaction_timestamp before cdl_effective_date minus 36 months
    AND transaction_timestamp >= add_months(cdl_effective_date, -36)
)

SELECT
  brand_normalized_name,
  normalized_name,
  market_normalized_name,
  competitor_flag,
  channel_name,
  transaction_timestamp,
  SUM(integrated_units) AS integrated_units,
  SUM(integrated_normalized_units) AS integrated_normalized_units,
  SUM(integrated_dollars) AS integrated_dollars
FROM bai_sales_cte
GROUP BY
  brand_normalized_name,
  normalized_name,
  market_normalized_name,
  competitor_flag,
  channel_name,
  transaction_timestamp
;
-- End of production SQL for bai_sales update

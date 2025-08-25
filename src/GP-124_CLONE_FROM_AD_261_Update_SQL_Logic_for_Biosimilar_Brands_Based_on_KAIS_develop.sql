USE CATALOG purgo_databricks;

/* 
==========================================================================================
Databricks SQL Implementation: bai_sales.sql Logic Update
==========================================================================================
Catalog: purgo_databricks
Schema: purgo_playground
Source Table: purgo_playground.bai_sales_agg_obu_customer_datapack_weekly

Requirements Implemented:
- Suppress integrated_units, integrated_normalized_units, integrated_dollars for biosimilar brands ("Kanjinti", "Mvasi", "Riabni") ONLY when br_gpo_flag contains "KAISER" (case-insensitive, NULL treated as empty string, trims spaces)
- Add competitor_flag to SELECT, cast as STRING
- Proper grouping including competitor_flag (cast as STRING)
- Data type validation and conversion
- Handles NULLs and edge cases
- No temp views/tables, CTE used only if required (not needed for direct query)
==========================================================================================
*/

SELECT
  brand_normalized_name,
  normalized_name,
  market_normalized_name,
  channel_name,
  transaction_timestamp,
  CAST(competitor_flag AS STRING) AS competitor_flag,
  SUM(
    CASE
      WHEN lower(brand_normalized_name) IN ('kanjinti','mvasi','riabni')
        AND instr(lower(trim(coalesce(br_gpo_flag, ''))), 'kaiser') > 0
      THEN 0
      ELSE integrated_units
    END
  ) AS integrated_units,
  SUM(
    CASE
      WHEN lower(brand_normalized_name) IN ('kanjinti','mvasi','riabni')
        AND instr(lower(trim(coalesce(br_gpo_flag, ''))), 'kaiser') > 0
      THEN 0
      ELSE integrated_normalized_units
    END
  ) AS integrated_normalized_units,
  SUM(
    CASE
      WHEN lower(brand_normalized_name) IN ('kanjinti','mvasi','riabni')
        AND instr(lower(trim(coalesce(br_gpo_flag, ''))), 'kaiser') > 0
      THEN 0
      ELSE integrated_dollars
    END
  ) AS integrated_dollars
FROM
  purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
WHERE
  CAST(transaction_timestamp AS DATE) >= add_months(cdl_effective_date, -36)
  AND lower(market_normalized_name) IN ('trastuzumab-anns','bevacizumab-awwb','rituximab market')
GROUP BY
  brand_normalized_name,
  normalized_name,
  market_normalized_name,
  channel_name,
  transaction_timestamp,
  CAST(competitor_flag AS STRING)
;
-- End of production logic for bai_sales.sql update

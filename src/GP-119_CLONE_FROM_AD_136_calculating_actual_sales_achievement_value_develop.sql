USE CATALOG purgo_databricks;

/*
  Databricks SQL Query: Calculate actual_value for each brand_name, country_code, territory_id, month, and year
  - Sums sales_value from t3_itm_territory_sales and sales_net_price_local from t3_ttm_territory_sales
  - Joins on country_code, brand_name, territory_id, month, year
  - Only includes records where source_system_name from both tables exist in control_table.source_system
  - Excludes records with NULL source_system_name, NULL sales_month/fiscal_date, or invalid sales_month format
  - Output columns: brand_name, country_code, territory_id, month, year, actual_value
*/

WITH valid_sales AS (
  SELECT
    itm.brand_name AS brand_name,
    itm.country_code AS country_code,
    itm.territory_id AS territory_id,
    CAST(SPLIT(itm.sales_month, '-')[1] AS INT) AS month,
    CAST(SPLIT(itm.sales_month, '-')[0] AS INT) AS year,
    itm.sales_value AS sales_value,
    ttm.sales_net_price_local AS sales_net_price_local
  FROM purgo_playground.t3_itm_territory_sales itm
  INNER JOIN purgo_playground.t3_ttm_territory_sales ttm
    ON itm.country_code = ttm.country_code
    AND itm.brand_name = ttm.brand_name
    AND itm.territory_id = ttm.territory_id
    AND CAST(SPLIT(itm.sales_month, '-')[1] AS INT) = MONTH(ttm.fiscal_date)
    AND CAST(SPLIT(itm.sales_month, '-')[0] AS INT) = YEAR(ttm.fiscal_date)
  WHERE
    itm.source_system_name IS NOT NULL
    AND ttm.source_system_name IS NOT NULL
    AND itm.sales_month IS NOT NULL
    AND ttm.fiscal_date IS NOT NULL
    AND itm.source_system_name IN (SELECT source_system FROM purgo_playground.control_table)
    AND ttm.source_system_name IN (SELECT source_system FROM purgo_playground.control_table)
    AND LENGTH(itm.sales_month) = 7
    AND SUBSTRING(itm.sales_month, 5, 1) = '-'
)

SELECT
  brand_name,
  country_code,
  territory_id,
  month,
  year,
  ROUND(SUM(sales_value + sales_net_price_local), 2) AS actual_value
FROM valid_sales
GROUP BY brand_name, country_code, territory_id, month, year
ORDER BY year DESC, month DESC, country_code, brand_name, territory_id;
-- End of actual_value calculation query

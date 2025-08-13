USE CATALOG purgo_databricks;

-- Test data generation for purgo_playground.bai_sales_agg_obu_customer_datapack_weekly
-- Covers happy path, edge cases, error cases, NULLs, special/multibyte chars

WITH test_data AS (
  SELECT
    -- Happy path: competitor_flag true, brand not suppressed
    'avastin' AS brand_normalized_name,
    'Avastin®' AS normalized_name,
    'bevacizumab-awwb' AS market_normalized_name,
    TRUE AS competitor_flag,
    'Hospital' AS channel_name,
    CAST('2024-05-01T00:00:00.000+0000' AS TIMESTAMP) AS transaction_timestamp,
    100 AS integrated_units,
    90 AS integrated_normalized_units,
    1000.0 AS integrated_dollars,
    'Y' AS br_gpo_flag,
    CAST('2021-05-01' AS DATE) AS cdl_effective_date
  UNION ALL
  -- Happy path: competitor_flag false, brand suppressed (riabni)
  SELECT
    'riabni',
    'Riabni™',
    'rituximab market',
    FALSE,
    'Retail',
    CAST('2024-05-01T00:00:00.000+0000' AS TIMESTAMP),
    0,
    0,
    0.0,
    'N',
    CAST('2021-05-01' AS DATE)
  UNION ALL
  -- Edge: competitor_flag NULL, brand suppressed (mvasi)
  SELECT
    'mvasi',
    'Mvasi®',
    'bevacizumab-awwb',
    NULL,
    'Specialty',
    CAST('2024-05-01T00:00:00.000+0000' AS TIMESTAMP),
    0,
    0,
    0.0,
    NULL,
    CAST('2021-05-01' AS DATE)
  UNION ALL
  -- Edge: brand suppressed (kanjinti), competitor_flag true
  SELECT
    'kanjinti',
    'Kanjinti®',
    'trastuzumab-anns',
    TRUE,
    'Hospital',
    CAST('2024-05-01T00:00:00.000+0000' AS TIMESTAMP),
    0,
    0,
    0.0,
    'Y',
    CAST('2021-05-01' AS DATE)
  UNION ALL
  -- Error: transaction_timestamp < add_months(cdl_effective_date, -36) (excluded)
  SELECT
    'avastin',
    'Avastin®',
    'bevacizumab-awwb',
    TRUE,
    'Hospital',
    CAST('2020-01-01T00:00:00.000+0000' AS TIMESTAMP),
    100,
    90,
    1000.0,
    'Y',
    CAST('2021-05-01' AS DATE)
  UNION ALL
  -- Error: market_normalized_name not in allowed list (excluded)
  SELECT
    'avastin',
    'Avastin®',
    'other-market',
    TRUE,
    'Hospital',
    CAST('2024-05-01T00:00:00.000+0000' AS TIMESTAMP),
    100,
    90,
    1000.0,
    'Y',
    CAST('2021-05-01' AS DATE)
  UNION ALL
  -- Error: brand_normalized_name is 'xyz' (not suppressed)
  SELECT
    'xyz',
    'XYZ Brand',
    'bevacizumab-awwb',
    FALSE,
    'Retail',
    CAST('2024-05-01T00:00:00.000+0000' AS TIMESTAMP),
    500,
    450,
    5000.0,
    'N',
    CAST('2021-05-01' AS DATE)
  UNION ALL
  -- Edge: competitor_flag false, brand not suppressed
  SELECT
    'herceptin',
    'Herceptin®',
    'trastuzumab-anns',
    FALSE,
    'Specialty',
    CAST('2024-05-01T00:00:00.000+0000' AS TIMESTAMP),
    200,
    180,
    2000.0,
    'Y',
    CAST('2021-05-01' AS DATE)
  UNION ALL
  -- Edge: competitor_flag true, brand suppressed (riabni)
  SELECT
    'riabni',
    'Riabni™',
    'rituximab market',
    TRUE,
    'Hospital',
    CAST('2024-05-01T00:00:00.000+0000' AS TIMESTAMP),
    0,
    0,
    0.0,
    'Y',
    CAST('2021-05-01' AS DATE)
  UNION ALL
  -- Edge: competitor_flag false, brand suppressed (mvasi)
  SELECT
    'mvasi',
    'Mvasi®',
    'bevacizumab-awwb',
    FALSE,
    'Retail',
    CAST('2024-05-01T00:00:00.000+0000' AS TIMESTAMP),
    0,
    0,
    0.0,
    'N',
    CAST('2021-05-01' AS DATE)
  UNION ALL
  -- Edge: competitor_flag true, brand suppressed (kanjinti)
  SELECT
    'kanjinti',
    'Kanjinti®',
    'trastuzumab-anns',
    TRUE,
    'Specialty',
    CAST('2024-05-01T00:00:00.000+0000' AS TIMESTAMP),
    0,
    0,
    0.0,
    'Y',
    CAST('2021-05-01' AS DATE)
  UNION ALL
  -- Edge: competitor_flag NULL, brand not suppressed
  SELECT
    'avastin',
    'Avastin®',
    'bevacizumab-awwb',
    NULL,
    'Hospital',
    CAST('2024-05-01T00:00:00.000+0000' AS TIMESTAMP),
    100,
    90,
    1000.0,
    NULL,
    CAST('2021-05-01' AS DATE)
  UNION ALL
  -- Edge: special/multibyte chars in normalized_name
  SELECT
    'avastin',
    'Avaßtin® 漢字',
    'bevacizumab-awwb',
    TRUE,
    'Hospital',
    CAST('2024-05-01T00:00:00.000+0000' AS TIMESTAMP),
    100,
    90,
    1000.0,
    'Y',
    CAST('2021-05-01' AS DATE)
  UNION ALL
  -- Edge: NULLs in integrated_units, integrated_normalized_units, integrated_dollars
  SELECT
    'avastin',
    'Avastin®',
    'bevacizumab-awwb',
    TRUE,
    'Hospital',
    CAST('2024-05-01T00:00:00.000+0000' AS TIMESTAMP),
    NULL,
    NULL,
    NULL,
    'Y',
    CAST('2021-05-01' AS DATE)
  UNION ALL
  -- Edge: competitor_flag false, brand suppressed (riabni), special chars
  SELECT
    'riabni',
    'Riabni™ 漢字',
    'rituximab market',
    FALSE,
    'Retail',
    CAST('2024-05-01T00:00:00.000+0000' AS TIMESTAMP),
    0,
    0,
    0.0,
    'N',
    CAST('2021-05-01' AS DATE)
  UNION ALL
  -- Edge: competitor_flag true, brand not suppressed, special chars
  SELECT
    'avastin',
    'Avastin® 漢字',
    'bevacizumab-awwb',
    TRUE,
    'Specialty',
    CAST('2024-05-01T00:00:00.000+0000' AS TIMESTAMP),
    100,
    90,
    1000.0,
    'Y',
    CAST('2021-05-01' AS DATE)
  UNION ALL
  -- Edge: competitor_flag false, brand not suppressed, special chars
  SELECT
    'herceptin',
    'Herceptin® 漢字',
    'trastuzumab-anns',
    FALSE,
    'Hospital',
    CAST('2024-05-01T00:00:00.000+0000' AS TIMESTAMP),
    200,
    180,
    2000.0,
    'Y',
    CAST('2021-05-01' AS DATE)
  UNION ALL
  -- Edge: competitor_flag NULL, brand suppressed (riabni)
  SELECT
    'riabni',
    'Riabni™',
    'rituximab market',
    NULL,
    'Specialty',
    CAST('2024-05-01T00:00:00.000+0000' AS TIMESTAMP),
    0,
    0,
    0.0,
    NULL,
    CAST('2021-05-01' AS DATE)
  UNION ALL
  -- Edge: competitor_flag NULL, brand not suppressed, NULLs in units
  SELECT
    'avastin',
    'Avastin®',
    'bevacizumab-awwb',
    NULL,
    'Retail',
    CAST('2024-05-01T00:00:00.000+0000' AS TIMESTAMP),
    NULL,
    NULL,
    NULL,
    NULL,
    CAST('2021-05-01' AS DATE)
  UNION ALL
  -- Edge: competitor_flag true, brand suppressed (mvasi), special chars
  SELECT
    'mvasi',
    'Mvasi® 漢字',
    'bevacizumab-awwb',
    TRUE,
    'Hospital',
    CAST('2024-05-01T00:00:00.000+0000' AS TIMESTAMP),
    0,
    0,
    0.0,
    'Y',
    CAST('2021-05-01' AS DATE)
  UNION ALL
  -- Edge: competitor_flag false, brand suppressed (kanjinti), special chars
  SELECT
    'kanjinti',
    'Kanjinti® 漢字',
    'trastuzumab-anns',
    FALSE,
    'Specialty',
    CAST('2024-05-01T00:00:00.000+0000' AS TIMESTAMP),
    0,
    0,
    0.0,
    'N',
    CAST('2021-05-01' AS DATE)
  UNION ALL
  -- Edge: competitor_flag true, brand not suppressed, NULLs in units
  SELECT
    'avastin',
    'Avastin®',
    'bevacizumab-awwb',
    TRUE,
    'Hospital',
    CAST('2024-05-01T00:00:00.000+0000' AS TIMESTAMP),
    NULL,
    NULL,
    NULL,
    'Y',
    CAST('2021-05-01' AS DATE)
  UNION ALL
  -- Edge: competitor_flag false, brand not suppressed, NULLs in units
  SELECT
    'herceptin',
    'Herceptin®',
    'trastuzumab-anns',
    FALSE,
    'Retail',
    CAST('2024-05-01T00:00:00.000+0000' AS TIMESTAMP),
    NULL,
    NULL,
    NULL,
    'N',
    CAST('2021-05-01' AS DATE)
)

-- Final output logic as per requirements
SELECT
  brand_normalized_name,
  normalized_name,
  market_normalized_name,
  CAST(competitor_flag AS STRING) AS competitor_flag,
  channel_name,
  transaction_timestamp,
  SUM(CASE WHEN lower(brand_normalized_name) IN ('kanjinti','mvasi','riabni')
           THEN 0 ELSE COALESCE(integrated_units, 0) END) AS integrated_units,
  SUM(CASE WHEN lower(brand_normalized_name) IN ('kanjinti','mvasi','riabni')
           THEN 0 ELSE COALESCE(integrated_normalized_units, 0) END) AS integrated_normalized_units,
  SUM(CASE WHEN lower(brand_normalized_name) IN ('kanjinti','mvasi','riabni')
           THEN 0 ELSE COALESCE(integrated_dollars, 0.0) END) AS integrated_dollars
FROM test_data
WHERE substring(CAST(transaction_timestamp AS STRING),1,10) >= CAST(add_months(cdl_effective_date, -36) AS STRING)
  AND lower(market_normalized_name) IN ('trastuzumab-anns','bevacizumab-awwb','rituximab market')
GROUP BY
  brand_normalized_name,
  normalized_name,
  market_normalized_name,
  CAST(competitor_flag AS STRING),
  channel_name,
  transaction_timestamp
;

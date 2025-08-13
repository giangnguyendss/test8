USE CATALOG purgo_databricks;

-- Test Data Generation for purgo_databricks.purgo_playground.bai_sales_agg_obu_customer_datapack_weekly

WITH test_data AS (
  SELECT -- Happy path: competitor_flag = FALSE, brand_normalized_name not suppressed
    'abc' AS brand_normalized_name,
    'ABC Normalized' AS normalized_name,
    'trastuzumab-anns' AS market_normalized_name,
    FALSE AS competitor_flag,
    'Hospital' AS channel_name,
    TIMESTAMP('2024-03-21T00:00:00.000+0000') AS transaction_timestamp,
    100 AS integrated_units,
    90 AS integrated_normalized_units,
    1000.50 AS integrated_dollars,
    'N' AS br_gpo_flag,
    DATE('2021-03-21') AS cdl_effective_date
  UNION ALL
  SELECT -- Suppression by brand_normalized_name = 'kanjinti'
    'kanjinti', 'KANJINTI Normalized', 'trastuzumab-anns', FALSE, 'Retail',
    TIMESTAMP('2024-03-22T00:00:00.000+0000'), 200, 180, 2000.00, 'Y', DATE('2021-03-22')
  UNION ALL
  SELECT -- Suppression by brand_normalized_name = 'mvasi'
    'mvasi', 'MVASI Normalized', 'bevacizumab-awwb', FALSE, 'Hospital',
    TIMESTAMP('2024-03-23T00:00:00.000+0000'), 300, 270, 3000.00, 'N', DATE('2021-03-23')
  UNION ALL
  SELECT -- Suppression by brand_normalized_name = 'riabni'
    'riabni', 'RIABNI Normalized', 'rituximab market', FALSE, 'Retail',
    TIMESTAMP('2024-03-24T00:00:00.000+0000'), 400, 360, 4000.00, 'Y', DATE('2021-03-24')
  UNION ALL
  SELECT -- Suppression by competitor_flag = TRUE
    'abc', 'ABC Normalized', 'trastuzumab-anns', TRUE, 'Hospital',
    TIMESTAMP('2024-03-25T00:00:00.000+0000'), 500, 450, 5000.00, 'N', DATE('2021-03-25')
  UNION ALL
  SELECT -- Suppression by both brand_normalized_name = 'riabni' and competitor_flag = TRUE
    'riabni', 'RIABNI Normalized', 'rituximab market', TRUE, 'Retail',
    TIMESTAMP('2024-03-26T00:00:00.000+0000'), 600, 540, 6000.00, 'Y', DATE('2021-03-26')
  UNION ALL
  SELECT -- Edge: competitor_flag = NULL
    'abc', 'ABC Normalized', 'bevacizumab-awwb', NULL, 'Hospital',
    TIMESTAMP('2024-03-27T00:00:00.000+0000'), 700, 630, 7000.00, 'N', DATE('2021-03-27')
  UNION ALL
  SELECT -- Edge: brand_normalized_name = 'RIABNI' (case insensitivity)
    'RIABNI', 'RIABNI Normalized', 'rituximab market', FALSE, 'Retail',
    TIMESTAMP('2024-03-28T00:00:00.000+0000'), 800, 720, 8000.00, 'Y', DATE('2021-03-28')
  UNION ALL
  SELECT -- Edge: market_normalized_name not in filter list
    'abc', 'ABC Normalized', 'other-market', FALSE, 'Hospital',
    TIMESTAMP('2024-03-29T00:00:00.000+0000'), 900, 810, 9000.00, 'N', DATE('2021-03-29')
  UNION ALL
  SELECT -- Edge: transaction_timestamp at boundary (exactly 36 months before cdl_effective_date)
    'abc', 'ABC Normalized', 'trastuzumab-anns', FALSE, 'Retail',
    TIMESTAMP('2018-03-21T00:00:00.000+0000'), 1000, 900, 10000.00, 'Y', DATE('2021-03-21')
  UNION ALL
  SELECT -- Error: cdl_effective_date is NULL
    'abc', 'ABC Normalized', 'bevacizumab-awwb', FALSE, 'Hospital',
    TIMESTAMP('2024-03-30T00:00:00.000+0000'), 1100, 990, 11000.00, 'N', NULL
  UNION ALL
  SELECT -- Error: cdl_effective_date is invalid (future date)
    'abc', 'ABC Normalized', 'rituximab market', FALSE, 'Retail',
    TIMESTAMP('2024-03-31T00:00:00.000+0000'), 1200, 1080, 12000.00, 'Y', DATE('2099-12-31')
  UNION ALL
  SELECT -- Special characters in brand_normalized_name
    'abc-漢字', 'ABC Special', 'trastuzumab-anns', FALSE, 'Hospital',
    TIMESTAMP('2024-04-01T00:00:00.000+0000'), 1300, 1170, 13000.00, 'N', DATE('2021-04-01')
  UNION ALL
  SELECT -- Special characters in normalized_name
    'abc', 'Nørmålïzéd', 'bevacizumab-awwb', FALSE, 'Retail',
    TIMESTAMP('2024-04-02T00:00:00.000+0000'), 1400, 1260, 14000.00, 'Y', DATE('2021-04-02')
  UNION ALL
  SELECT -- NULL in integrated_units
    'abc', 'ABC Normalized', 'rituximab market', FALSE, 'Hospital',
    TIMESTAMP('2024-04-03T00:00:00.000+0000'), NULL, 1350, 15000.00, 'N', DATE('2021-04-03')
  UNION ALL
  SELECT -- NULL in integrated_normalized_units
    'abc', 'ABC Normalized', 'trastuzumab-anns', FALSE, 'Retail',
    TIMESTAMP('2024-04-04T00:00:00.000+0000'), 1600, NULL, 16000.00, 'Y', DATE('2021-04-04')
  UNION ALL
  SELECT -- NULL in integrated_dollars
    'abc', 'ABC Normalized', 'bevacizumab-awwb', FALSE, 'Hospital',
    TIMESTAMP('2024-04-05T00:00:00.000+0000'), 1700, 1530, NULL, 'N', DATE('2021-04-05')
  UNION ALL
  SELECT -- All NULLs except required fields
    NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL
  UNION ALL
  SELECT -- Edge: competitor_flag = TRUE, brand_normalized_name = 'mvasi'
    'mvasi', 'MVASI Normalized', 'bevacizumab-awwb', TRUE, 'Retail',
    TIMESTAMP('2024-04-06T00:00:00.000+0000'), 1800, 1620, 18000.00, 'Y', DATE('2021-04-06')
  UNION ALL
  SELECT -- Edge: competitor_flag = FALSE, brand_normalized_name = 'riabni'
    'riabni', 'RIABNI Normalized', 'rituximab market', FALSE, 'Hospital',
    TIMESTAMP('2024-04-07T00:00:00.000+0000'), 1900, 1710, 19000.00, 'N', DATE('2021-04-07')
  UNION ALL
  SELECT -- Edge: competitor_flag = TRUE, brand_normalized_name = 'kanjinti'
    'kanjinti', 'KANJINTI Normalized', 'trastuzumab-anns', TRUE, 'Retail',
    TIMESTAMP('2024-04-08T00:00:00.000+0000'), 2000, 1800, 20000.00, 'Y', DATE('2021-04-08')
  UNION ALL
  SELECT -- Edge: competitor_flag = NULL, brand_normalized_name = 'mvasi'
    'mvasi', 'MVASI Normalized', 'bevacizumab-awwb', NULL, 'Hospital',
    TIMESTAMP('2024-04-09T00:00:00.000+0000'), 2100, 1890, 21000.00, 'N', DATE('2021-04-09')
  UNION ALL
  SELECT -- Edge: competitor_flag = NULL, brand_normalized_name = 'riabni'
    'riabni', 'RIABNI Normalized', 'rituximab market', NULL, 'Retail',
    TIMESTAMP('2024-04-10T00:00:00.000+0000'), 2200, 1980, 22000.00, 'Y', DATE('2021-04-10')
  UNION ALL
  SELECT -- Edge: competitor_flag = TRUE, brand_normalized_name = 'abc'
    'abc', 'ABC Normalized', 'trastuzumab-anns', TRUE, 'Hospital',
    TIMESTAMP('2024-04-11T00:00:00.000+0000'), 2300, 2070, 23000.00, 'N', DATE('2021-04-11')
  UNION ALL
  SELECT -- Edge: competitor_flag = FALSE, brand_normalized_name = 'abc'
    'abc', 'ABC Normalized', 'bevacizumab-awwb', FALSE, 'Retail',
    TIMESTAMP('2024-04-12T00:00:00.000+0000'), 2400, 2160, 24000.00, 'Y', DATE('2021-04-12')
  UNION ALL
  SELECT -- Edge: competitor_flag = TRUE, brand_normalized_name = 'riabni'
    'riabni', 'RIABNI Normalized', 'rituximab market', TRUE, 'Hospital',
    TIMESTAMP('2024-04-13T00:00:00.000+0000'), 2500, 2250, 25000.00, 'N', DATE('2021-04-13')
  UNION ALL
  SELECT -- Special: multi-byte in channel_name
    'abc', 'ABC Normalized', 'trastuzumab-anns', FALSE, '零售',
    TIMESTAMP('2024-04-14T00:00:00.000+0000'), 2600, 2340, 26000.00, 'Y', DATE('2021-04-14')
  UNION ALL
  SELECT -- Special: special characters in market_normalized_name
    'abc', 'ABC Normalized', 'bévacizumab-awwb', FALSE, 'Hospital',
    TIMESTAMP('2024-04-15T00:00:00.000+0000'), 2700, 2430, 27000.00, 'N', DATE('2021-04-15')
  UNION ALL
  SELECT -- Special: competitor_flag = NULL, all other fields valid
    'abc', 'ABC Normalized', 'rituximab market', NULL, 'Retail',
    TIMESTAMP('2024-04-16T00:00:00.000+0000'), 2800, 2520, 28000.00, 'Y', DATE('2021-04-16')
)

SELECT * FROM test_data
;

-- ============================================================================
-- Databricks SQL: Upsert Excel Data into metric_config and metric_master Tables
-- ----------------------------------------------------------------------------
-- This script performs upsert (insert/update) operations for metric_config and
-- metric_master tables in Unity Catalog purgo_databricks.purgo_playground
-- using data from the provided Excel sheet.
-- ----------------------------------------------------------------------------
-- - For metric_config: Upsert by metric_id
-- - For metric_master: Upsert by (metric_template_name, dependency) using NULL-safe match
-- - Ensures column order, count, and data types match target table schema
-- - Handles NULLs and empty strings as per Databricks best practices
-- - No temp views or temp tables used; CTEs only
-- - No Delta table operations unless explicitly requested
-- ============================================================================

USE CATALOG purgo_databricks;

-- =========================
-- Upsert for metric_config
-- =========================
MERGE INTO purgo_databricks.purgo_playground.metric_config AS target
USING (
  -- CTE: Excel data for metric_config
  SELECT
    'evenity_unit_hash' AS metric_id,
    'Y' AS active_indicator,
    NULL AS geographical_average_type,
    'String' AS metric_data_type,
    'hash value of product level , product name and market ' AS metric_description,
    'evenity_unit_hash' AS metric_template_name,
    'Custom' AS metric_type,
    NULL AS optional_filters,
    'purgo_playground.s_field_reporting_sales_source_customer' AS source_table,
    'customer' AS table_type,
    'purgo_playground.s_field_reporting_sales_source_customer_metric' AS target_table,
    NULL AS template_parameters,
    NULL AS bu_filter,
    NULL AS calling_service_name,
    'pt_cdl_uuid' AS primary_key
  UNION ALL
  SELECT
    'customer_first_metric',
    'Y',
    NULL,
    NULL,
    'First purchased columns of a customer',
    'customer_first_metric',
    'Custom',
    NULL,
    'purgo_playground.d_product_revenue',
    'Product',
    'purgo_playground.d_product_revenue_metric',
    NULL,
    NULL,
    NULL,
    'product_id'
  UNION ALL
  SELECT
    'final_css',
    'Y',
    NULL,
    'Double',
    'Normalised Customer Satisfaction Score',
    'final_css',
    'Custom',
    NULL,
    'purgo_playground.health_insurance_claims',
    'HCP',
    'purgo_playground.health_insurance_claims_metric',
    NULL,
    NULL,
    NULL,
    'Claim_ID'
  UNION ALL
  SELECT
    'batch_number',
    'N',
    NULL,
    'String',
    'Batch number of product',
    'batch_number',
    'Custom',
    NULL,
    'purgo_playground.d_product_revenue',
    'Product',
    'purgo_playground.d_product_revenue_metric',
    NULL,
    NULL,
    NULL,
    'product_id'
) AS source
ON target.metric_id = source.metric_id
WHEN MATCHED THEN
  UPDATE SET
    active_indicator = source.active_indicator,
    geographical_average_type = source.geographical_average_type,
    metric_data_type = source.metric_data_type,
    metric_description = source.metric_description,
    metric_template_name = source.metric_template_name,
    metric_type = source.metric_type,
    optional_filters = source.optional_filters,
    source_table = source.source_table,
    table_type = source.table_type,
    target_table = source.target_table,
    template_parameters = source.template_parameters,
    bu_filter = source.bu_filter,
    calling_service_name = source.calling_service_name,
    primary_key = source.primary_key
WHEN NOT MATCHED THEN
  INSERT (
    metric_id,
    active_indicator,
    geographical_average_type,
    metric_data_type,
    metric_description,
    metric_template_name,
    metric_type,
    optional_filters,
    source_table,
    table_type,
    target_table,
    template_parameters,
    bu_filter,
    calling_service_name,
    primary_key
  )
  VALUES (
    source.metric_id,
    source.active_indicator,
    source.geographical_average_type,
    source.metric_data_type,
    source.metric_description,
    source.metric_template_name,
    source.metric_type,
    source.optional_filters,
    source.source_table,
    source.table_type,
    source.target_table,
    source.template_parameters,
    source.bu_filter,
    source.calling_service_name,
    source.primary_key
  );

-- =========================
-- Upsert for metric_master
-- =========================
MERGE INTO purgo_databricks.purgo_playground.metric_master AS target
USING (
  -- CTE: Excel data for metric_master
  SELECT
    'evenity_unit_hash' AS metric_template_name,
    'SELECT * FROM (SELECT source_customer.*, time_bucket.time_bucket_start_date, time_bucket.time_bucket_end_date FROM agilisium_playground.purgo_playground.s_field_reporting_sales_source_customer source_customer INNER JOIN agilisium_playground.purgo_playground.s_field_reporting_sales_time_group_bucket time_bucket ON source_customer.pt_cdl_uuid = time_bucket.pt_cdl_uuid AND LOWER(source_customer.time_bucket_id) = LOWER(time_bucket.time_bucket_id) AND LOWER(source_customer.cdl_frequency) = LOWER(time_bucket.cdl_frequency) AND LOWER(source_customer.field_force_code) = LOWER(time_bucket.field_force_code) WHERE product_name = ''EVENITY'' AND product_level = ''BRAND'' AND market_name = ''PMO TOTAL - BHBU'')' AS sql_query,
    'Custom' AS metric_type,
    'eve_view' AS view_name,
    1 AS dependency
  UNION ALL
  SELECT
    'customer_first_metric',
    'SELECT product_id, FIRST_VALUE(purchased_date) OVER (PARTITION BY customer_id ORDER BY purchased_date) AS first_purchase_date, FIRST_VALUE(product_id) OVER (PARTITION BY customer_id ORDER BY purchased_date) AS first_purchase_product, FIRST_VALUE(revenue) OVER (PARTITION BY customer_id ORDER BY purchased_date) AS first_revenue FROM purgo_playground.d_product_revenue;',
    'Custom',
    NULL,
    1
  UNION ALL
  SELECT
    'final_css',
    'SELECT *, CASE WHEN Billed_Amount = 0 THEN 0 ELSE (0.2 * (Allowed_Amount / Billed_Amount) * 100) - (0.3 * (Patient_Paid / Billed_Amount) * 100) - (0.2 * DATEDIFF(Service_Date, service_requested_date)) + (0.2 * SIZE(SPLIT(purchase_history, '',''))) - (0.1 * (1 - is_churn)) END AS CSS FROM joined_data;',
    'Custom',
    'css_data',
    2
  UNION ALL
  SELECT
    'final_css',
    'WITH css_stats AS (SELECT MIN(CSS) AS min_css, MAX(CSS) AS max_css FROM css_data) SELECT Claim_ID,CASE WHEN max_css = min_css THEN 0 ELSE ((CSS - min_css) / (max_css - min_css)) * 10 END AS final_CSS FROM css_data, css_stats;',
    'Custom',
    NULL,
    3
  UNION ALL
  SELECT
    'final_css',
    'SELECT h.*, c.id, c.purchase_history, c.is_churn FROM purgo_playground.health_insurance_claims h JOIN purgo_playground.customer_360_raw_with_churn c ON h.Patient_ID = c.id;',
    'Custom',
    'joined_data',
    1
  UNION ALL
  SELECT
    'evenity_unit_hash',
    'SELECT pt_cdl_uuid, HASH(CONCAT_WS("_","PGP",market_name,product_level,product_name)) AS evenity_unit_hash FROM eve_view',
    'Custom',
    NULL,
    2
  UNION ALL
  SELECT
    'batch_number',
    'SELECT product_id, get_json_object(product_details, "$.batch_number") AS batch_number FROM purgo_playground.d_product_revenue',
    'Custom',
    NULL,
    1
) AS source
ON target.metric_template_name <=> source.metric_template_name AND target.dependency <=> source.dependency
WHEN MATCHED THEN
  UPDATE SET
    sql_query = source.sql_query,
    metric_type = source.metric_type,
    view_name = source.view_name,
    dependency = source.dependency
WHEN NOT MATCHED THEN
  INSERT (
    metric_template_name,
    sql_query,
    metric_type,
    view_name,
    dependency
  )
  VALUES (
    source.metric_template_name,
    source.sql_query,
    source.metric_type,
    source.view_name,
    source.dependency
  );
-- End of upsert script


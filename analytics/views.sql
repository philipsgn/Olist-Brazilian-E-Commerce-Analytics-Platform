-- ==========================================
-- analytics/views.sql
-- PHASE 5: DATA MODELING & GOLD LAYER (LAMBDA ARCHITECTURE)
-- ==========================================
-- Project  : Olist Brazilian E-Commerce Analytics Platform
-- Author   : TanPhat
-- Target   : AWS Athena (AwsDataCatalog)
-- ==========================================

-- ------------------------------------------
-- 1. GOLD LAYER VIEW (LAMBDA ARCHITECTURE)
--    Combines Real-time Streaming (S3) and Batch (RDS) 
--    into a single denormalized analytics view.
-- ------------------------------------------
CREATE OR REPLACE VIEW "AwsDataCatalog"."default"."view_order_analytics_gold" AS 
SELECT
    'Streaming' as source,
    order_id,
    '1' as item_id,
    price,
    freight_value,
    payment_type,
    customer_state,
    order_status,
    CAST(from_iso8601_timestamp(created_at) AS timestamp) as created_at,
    total_amount,
    product
FROM
    "default"."marts_streaming_orders"

UNION ALL 

SELECT
    'Batch' as source,
    f.order_id,
    CAST(f.order_item_id AS VARCHAR) as item_id,
    f.price,
    f.freight_value,
    p.payment_types as payment_type,
    c.customer_state,
    'delivered' as order_status,
    CAST(f.order_purchase_timestamp AS timestamp) as created_at,
    (f.price + f.freight_value) as total_amount,
    COALESCE(d.product_category, 'others') as product
FROM
    "rds_postgres"."dev_pht"."fact_order_items" f
    LEFT JOIN "rds_postgres"."dev_pht"."fact_payments" p ON f.order_id = p.order_id
    LEFT JOIN "rds_postgres"."dev_pht"."dim_customers" c ON f.customer_id = c.customer_id
    LEFT JOIN "rds_postgres"."dev_pht"."dim_products"  d ON f.product_id  = d.product_id;

-- ------------------------------------------
-- 2. BUSINESS PERFORMANCE VIEW
--    Business metric: total revenue per customer state.
-- ------------------------------------------
CREATE OR REPLACE VIEW "AwsDataCatalog"."default"."view_state_revenue_metrics" AS
SELECT
    customer_state,
    COUNT(DISTINCT order_id)      AS total_orders,
    SUM(total_amount)             AS total_revenue,
    AVG(total_amount)             AS avg_order_value,
    COUNT(CASE WHEN source = 'Streaming' THEN 1 END) AS realtime_orders_count
FROM
    "default"."view_order_analytics_gold"
GROUP BY
    customer_state
ORDER BY
    total_revenue DESC;

-- ------------------------------------------
-- 3. AI SALES FORECAST TABLE (GOLD LAYER)
--    External table pointing to AI-generated Parquet on S3.
-- ------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`gold_sales_forecast` (
  `ds` timestamp,
  `yhat` double,
  `yhat_lower` double,
  `yhat_upper` double,
  `prediction_date` string
)
STORED AS PARQUET
LOCATION 's3://olist-de-tanphat-2026/gold/sales_forecast/'
TBLPROPERTIES ('classification'='parquet');

-- ------------------------------------------
-- 4. ACTUAL VS FORECAST UNIFIED VIEW
--    Combines historical Lambda data with AI predictions.
-- ------------------------------------------
CREATE OR REPLACE VIEW `default`.`view_actual_vs_forecast` AS
SELECT 
    date_trunc('day', created_at) AS event_date,
    SUM(total_amount)             AS actual_value,
    CAST(NULL AS DOUBLE)          AS forecast_value,
    'ACTUAL'                      AS data_type
FROM 
    "default"."view_order_analytics_gold"
GROUP BY 1

UNION ALL

SELECT 
    ds                            AS event_date,
    CAST(NULL AS DOUBLE)          AS actual_value,
    yhat                          AS forecast_value,
    'FORECAST'                    AS data_type
FROM 
    "default"."gold_sales_forecast";

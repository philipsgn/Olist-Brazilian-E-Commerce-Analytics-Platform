-- ==========================================
-- analytics/views.sql
-- PHASE 5: DATA MODELING & GOLD LAYER
-- ==========================================
-- Project  : Olist Brazilian E-Commerce Analytics Platform
-- Author   : TanPhat
-- Target   : AWS Athena (AwsDataCatalog)
-- Sources  :
--   [1] rds_postgres  -> AWS Glue Federated Query connector to RDS PostgreSQL
--   [2] AwsDataCatalog."default"."marts_streaming_orders" -> S3-backed Glue table
--
-- Architecture:
--   RDS PostgreSQL (fact/dim tables via dbt)
--       |
--       +--> Glue Federated Connector ("rds_postgres" data source)
--       |
--       v
--   Athena Gold View (view_order_analytics_gold)  <---+
--                                                     |
--   Kinesis Firehose --> S3 --> Glue Table            |
--   (marts_streaming_orders)  ------------------------+
--       |
--       v
--   Cross-Source Lakehouse Join Query (Section 2)
-- ==========================================


-- ------------------------------------------
-- 1. GOLD LAYER VIEW
--    Joins three RDS tables via Federated Query
--    into a single denormalized analytics view.
--    This view is registered in Glue Catalog and
--    queryable from Athena like a native table.
-- ------------------------------------------
CREATE OR REPLACE VIEW "AwsDataCatalog"."default"."view_order_analytics_gold" AS
SELECT
    f.order_id,
    c.customer_id,
    c.customer_state,
    p.product_id,
    p.product_category_name,
    f.price,
    f.freight_value,
    (f.price + f.freight_value) AS total_order_amount
FROM
    "rds_postgres"."dev_pht"."fact_order_items"  f
    JOIN "rds_postgres"."dev_pht"."dim_customers" c ON f.customer_id = c.customer_id
    JOIN "rds_postgres"."dev_pht"."dim_products"  p ON f.product_id  = p.product_id;


-- ------------------------------------------
-- 2. CROSS-SOURCE LAKEHOUSE JOIN
--    Demonstrates federated Lakehouse capability:
--    Gold view (RDS via Glue connector) is joined
--    with real-time streaming data stored on S3
--    (ingested via Kinesis Firehose -> streaming/simulator.py).
--
--    Use case: Enrich structured warehouse data with
--    the latest order status from the streaming layer.
-- ------------------------------------------
SELECT
    v.order_id,
    v.customer_state,
    v.product_category_name,
    v.total_order_amount,
    s.order_status           AS status_on_s3,
    s.review_score           AS streaming_review_score,
    s.created_at             AS streaming_arrival_time
FROM
    "AwsDataCatalog"."default"."view_order_analytics_gold"   v
    LEFT JOIN "AwsDataCatalog"."default"."marts_streaming_orders" s
        ON v.order_id = s.order_id
WHERE
    s.order_id IS NOT NULL   -- Only orders that appear in both sources
ORDER BY
    s.created_at DESC;


-- ------------------------------------------
-- 3. AGGREGATE: Revenue by State (Gold + Streaming)
--    Business metric: total revenue per customer state,
--    filtered to orders confirmed in both RDS and S3.
-- ------------------------------------------
SELECT
    v.customer_state,
    COUNT(DISTINCT v.order_id)      AS total_orders,
    SUM(v.total_order_amount)       AS total_revenue,
    AVG(v.total_order_amount)       AS avg_order_value,
    AVG(s.review_score)             AS avg_review_score
FROM
    "AwsDataCatalog"."default"."view_order_analytics_gold"   v
    JOIN "AwsDataCatalog"."default"."marts_streaming_orders" s
        ON v.order_id = s.order_id
GROUP BY
    v.customer_state
ORDER BY
    total_revenue DESC;

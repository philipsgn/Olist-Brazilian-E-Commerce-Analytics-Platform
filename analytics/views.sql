-- 1. GOLD LAYER VIEW (LAMBDA ARCHITECTURE - CLEAN VERSION)
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
    -- [REVERT] Bỏ cộng 8 năm vì dữ liệu rds_postgres đã ở năm 2026
    CAST(f.order_date_id AS timestamp) as created_at,
    (f.price + f.freight_value) as total_amount,
    COALESCE(d.product_category, 'others') as product
FROM
    "rds_postgres"."dev_pht"."fact_order_items" f
    LEFT JOIN "rds_postgres"."dev_pht"."fact_payments" p ON f.order_id = p.order_id
    LEFT JOIN "rds_postgres"."dev_pht"."dim_customers" c ON f.customer_id = c.customer_id
    LEFT JOIN "rds_postgres"."dev_pht"."dim_products"  d ON f.product_id  = d.product_id;

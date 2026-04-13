# Mới nhất 2026 DE

## Athena Federated Query (Thực tế triển khai)

- Không chạy Glue Crawler thủ công.
- RDS PostgreSQL được ánh xạ qua Athena Federated Query bằng Lambda Connector.
- Streaming S3 dùng Kinesis Data Firehose + Glue Data Catalog integration (hoặc Athena DDL thủ công) để có schema ngay.
- Schema thực tế đang dùng trong Athena: `"rds_postgres"."dev_pht"`.
- Cột thời gian batch dùng: `order_date_id`.

## Streaming Schema Discovery

> AWS Glue Data Catalog serves as the central metadata repository.
>
> Tables are managed via Kinesis Data Firehose Direct Integration (or Manual DDL in Athena), ensuring real-time schema availability without manual Glue Crawler runs.

## Gold Layer Query (Batch + Streaming)

```sql
SELECT
    'Streaming' AS source,
    s.order_id,
    CAST(s.created_at AS TIMESTAMP) AS created_at,
    (s.price + s.freight_value) AS total_amount
FROM "default"."marts_streaming_orders" s
UNION ALL
SELECT
    'Batch' AS source,
    f.order_id,
    CAST(f.order_date_id AS TIMESTAMP) AS created_at,
    (f.price + f.freight_value) AS total_amount
FROM "rds_postgres"."dev_pht"."fact_order_items" f;
```

## VPC Endpoints (Khuyến nghị giữ nguyên)

Nếu Lambda Connector chạy trong VPC, cần endpoint cho `Glue`, `Secrets Manager`, `S3` để tránh timeout khi Athena truy cập metadata và secrets.

-- Chạy script này trên giao diện AWS Athena (Query Editor)
-- Đảm bảo thay tên S3 bucket bằng bucket thật của bạn: olist-de-tanphat-2026

CREATE EXTERNAL TABLE IF NOT EXISTS default.mart_revenue_daily (
  date_day timestamp,
  product_category string,
  customer_state string,
  gross_revenue double,
  freight_cost double,
  total_revenue double,
  total_orders bigint,
  unique_customers bigint,
  avg_order_value double
)
PARTITIONED BY (
  year string,
  month string,
  day string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://olist-de-tanphat-2026/processed/mart_revenue_daily/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'projection.enabled'='true',
  'projection.year.type'='integer',
  'projection.year.range'='2016,2030',
  'projection.year.digits'='4',
  'projection.month.type'='integer',
  'projection.month.range'='1,12',
  'projection.month.digits'='2',
  'projection.day.type'='integer',
  'projection.day.range'='1,31',
  'projection.day.digits'='2',
  'storage.location.template'='s3://olist-de-tanphat-2026/processed/mart_revenue_daily/year=${year}/month=${month}/day=${day}/'
);

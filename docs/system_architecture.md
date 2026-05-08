# 🏛️ Olist Data Platform: System Architecture Documentation

## 1. Overview
Hệ thống được thiết kế dựa trên nguyên tắc **Serverless-First** và **Medallion Architecture**, đảm bảo khả năng mở rộng (scalability) và tối ưu chi phí (Cost Optimization). Dự án giải quyết bài toán hợp nhất dữ liệu từ nguồn Batch (PostgreSQL) và Streaming (Real-time Events).

## 2. High-Level Data Flow
1. **Streaming Data:** Lambda (Producer) -> Kinesis Firehose -> S3 Raw (Streaming).
2. **Batch Data:** Local CSV -> S3 Raw (Bronze).
3. **Transformation:** Airflow điều phối dbt chạy trên RDS PostgreSQL -> Export Gold Layer (Parquet) lên S3.
4. **Analytics:** Athena Query kết hợp S3 Gold & RDS qua Federated Query -> Superset Dashboard.

## 3. Detailed Components & Proof of Work
Tài liệu được chia nhỏ thành các học phần kỹ thuật chuyên sâu bên dưới. Mỗi học phần chứa giải trình kiến trúc và hình ảnh cấu hình thực tế:

- [🔐 01. Networking & Security](./01_networking_security/README.md): VPC, Endpoints, IAM, Secrets Manager.
- [📦 02. Storage & Data Lake](./02_storage_layer/README.md): S3 Medallion Layering, Lifecycle Policy.
- [⚡ 03. Streaming Ingestion](./03_streaming_ingestion/README.md): Lambda Architecture, Kinesis Firehose.
- [🔍 04. Data Catalog & Query Engine](./04_catalog_query/README.md): Glue, Athena, Partition Projection.
- [⚙️ 05. Orchestration & Visualization](./05_orchestration_analytics/README.md): Airflow, dbt, Superset.

---
**Author:** Tan Phat (philipsgn)
**Role:** Lead Data Engineer / AWS Architect

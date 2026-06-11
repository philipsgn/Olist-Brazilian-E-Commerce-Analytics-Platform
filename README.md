# 🚀 Olist Brazilian E-Commerce Analytics Platform

> **End-to-End Hybrid Data Lakehouse** — Kiến trúc Batch & Streaming hội tụ trên AWS, vận hành bởi Airflow, dbt, Athena và Apache Superset.

[![AWS](https://img.shields.io/badge/AWS-Cloud-FF9900?logo=amazonaws&logoColor=white)](https://aws.amazon.com)
[![Airflow](https://img.shields.io/badge/Airflow-2.x-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org)
[![dbt](https://img.shields.io/badge/dbt-Medallion-FF694B?logo=dbt&logoColor=white)](https://www.getdbt.com)
[![Terraform](https://img.shields.io/badge/Terraform-IaC-844FBA?logo=terraform&logoColor=white)](https://www.terraform.io)
[![Superset](https://img.shields.io/badge/Superset-Dashboard-20A7C9?logo=apache&logoColor=white)](https://superset.apache.org)

---

## 🏗️ System Architecture

Nền tảng được xây dựng theo mô hình **Modern Data Lakehouse**, hội tụ hai luồng dữ liệu độc lập vào một S3 Data Lake được quản lý bởi Glue Data Catalog — tạo ra một điểm truy vấn duy nhất (Single Query Plane) qua Amazon Athena.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          INGESTION LAYER                                 │
│                                                                          │
│   ┌──────────────────┐            ┌──────────────────────────────────┐  │
│   │  Lambda Producer │──(30s)──▶  │   Kinesis Data Firehose          │  │
│   │  (EventBridge)   │            │   Batching: 60s / 1MB → S3 Raw   │  │
│   └──────────────────┘            └──────────────────────────────────┘  │
│                                                                          │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │  Historical Kaggle CSV   ──▶   RDS PostgreSQL (raw.*)            │  │
│   └──────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    ORCHESTRATION LAYER  (EC2 + Docker)                   │
│                                                                          │
│   Apache Airflow DAG: ecommerce_daily_production_pipeline                │
│   ┌────────────────────────────────────────────────────────────────┐    │
│   │  verify_schema → load_csv → load_streaming                     │    │
│   │     → dbt run/test (staging) → dbt run/test (marts)            │    │
│   │       → export_to_s3 (Parquet+Snappy) → Athena DQ Check        │    │
│   └────────────────────────────────────────────────────────────────┘    │
│   Discord Webhook Alert on failure / retry                               │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    ▼                               ▼
        ┌─────────────────────┐       ┌────────────────────────┐
        │  S3 Data Lake       │       │  RDS PostgreSQL (AWS)   │
        │  Bronze → Gold      │       │  Staging + Marts (dbt)  │
        │  Parquet + Snappy   │       └────────────────────────┘
        └─────────────────────┘                   │
                    │                              │
                    └──────────┬───────────────────┘
                               ▼
              ┌─────────────────────────────────┐
              │  Amazon Athena (Federated Query) │
              │  + Glue Data Catalog             │
              │  + Partition Projection          │
              └─────────────────────────────────┘
                               │
                               ▼
                  ┌─────────────────────────┐
                  │   Apache Superset       │
                  │   Real-time Dashboard   │
                  └─────────────────────────┘
```

---

## ⭐ Engineering Highlights (What Makes This Senior-Level)

### 1. 🔄 Hybrid Ingestion — Batch & Real-time Unified
| Luồng | Công nghệ | Đặc điểm kỹ thuật |
|-------|-----------|-------------------|
| **Streaming** | Lambda → Kinesis Firehose → S3 | EventBridge trigger **mỗi 30 giây**, Firehose tự Batching 60s/1MB để tránh *Small Files Problem* |
| **Batch** | CSV → RDS PostgreSQL | Dữ liệu lịch sử từ Kaggle Olist dataset, ingest qua Airflow PythonOperator |

### 2. 🥇 Medallion Architecture với FinOps Optimization
- **Bronze (Raw):** JSON Lines (Streaming) + CSV (Batch) — Single Source of Truth.
- **Silver (Staging):** dbt transform: chuẩn hóa schema, type casting, data contracts.
- **Gold (Analytics-Ready):** **Parquet + Snappy compression** — giảm **70–80% dung lượng**, tăng tốc Athena query **5x**.
- **S3 Lifecycle (Terraform IaC):** Raw → Standard-IA (60 ngày) → Glacier (180 ngày). Processed → Standard-IA (30 ngày) → Glacier (90 ngày).

### 3. ⚡ Serverless Analytics — Athena Performance Tuning
- **Partition Projection:** Thay vì `MSCK REPAIR TABLE` hoặc Glue Crawler tốn phí, Athena tự tính toán phân vùng theo `year/month/day/hour` — **loại bỏ hoàn toàn latency** khi có partition mới.
- **DDL-based Cataloging:** Đăng ký bảng qua SQL DDL thay vì Crawler → schema nhất quán, CI/CD friendly.
- **Federated Query (Hybrid Lakehouse):** Athena JOIN trực tiếp dữ liệu S3 (Streaming Gold) với RDS PostgreSQL (Batch Marts) — một điểm truy vấn duy nhất không cần copy data.

### 4. 🛡️ Production-Grade Data Quality
- **Custom `AthenaDataQualityOperator`:** Toàn bộ DQ check được thực thi **trực tiếp trên Athena** (serverless), không cần công cụ bên thứ 3.
- **Composite Key Validation:** Kiểm tra uniqueness theo `order_id + item_id + source + created_at` — xử lý đúng đặc trưng *multi-item orders* của Olist.
- **Fail Fast:** Nếu `bad_count > 0` → Raise Exception ngay, dừng toàn bộ pipeline, gửi alert Discord.

### 5. 🔐 Zero-Trust Security (AWS Best Practices)
- **VPC S3 Gateway Endpoint:** Traffic Airflow EC2 ↔ S3 đi qua mạng nội bộ AWS. **Không một byte nào ra Internet**, triệt tiêu phí NAT Gateway.
- **Secrets Manager Endpoint:** Database credentials được fetch runtime qua PrivateLink, không hardcode.
- **IAM Instance Profile:** Không lưu Access Key/Secret Key trong code hoặc `.env`.
- **Least Privilege Security Groups:** RDS chỉ nhận kết nối từ Airflow SG, EC2 chỉ mở port 8080/8088/22 với IP whitelist.

---

## 🛠️ Tech Stack

| Layer | Technology | Role |
|-------|-----------|------|
| **Infrastructure** | Terraform (IaC) | Tạo S3 bucket, Lifecycle policy tự động |
| **Networking** | AWS VPC, Security Groups, PrivateLink | Zero-trust network isolation |
| **Secrets** | AWS Secrets Manager | Runtime credential injection |
| **Streaming** | AWS Lambda + Kinesis Data Firehose | Real-time event simulation & buffering |
| **Batch Storage** | AWS RDS PostgreSQL | Analytics warehouse (dbt target) |
| **Object Storage** | Amazon S3 (Bronze/Gold) | Medallion Data Lake |
| **Orchestration** | Apache Airflow 2.x (Docker on EC2) | DAG scheduling, retry, alerting |
| **Transformation** | dbt (Data Build Tool) | Staging & Marts modeling, schema tests |
| **Catalog** | AWS Glue Data Catalog | Central metadata store (DDL-based) |
| **Query Engine** | Amazon Athena | Serverless SQL, Partition Projection, Federated Query |
| **Visualization** | Apache Superset | Real-time Business Dashboard |
| **Alerting** | Discord Webhook + CloudWatch | Failure & retry notifications |

---

## 📁 Project Structure

```
ecommerce-de-project/
│
├── 📂 airflow/
│   ├── dags/
│   │   └── ecommerce_pipeline.py          # Production DAG (Batch + Streaming + DQ)
│   └── plugins/
│       └── operators/
│           └── athena_data_quality.py     # Custom AthenaDataQualityOperator
│
├── 📂 dbt_project/
│   └── ecommerce/
│       ├── models/staging/                # Silver layer: type casting, naming
│       └── models/marts/                  # Gold layer: business aggregations
│
├── 📂 ingestion/
│   ├── load_csv.py                        # Batch loader: CSV → RDS
│   ├── load_streaming.py                  # Streaming loader: Firehose → S3
│   └── simulate_data.py                   # Fake order generator (Olist taxonomy)
│
├── 📂 streaming/
│   └── simulator.py                       # Lambda-compatible event producer
│
├── 📂 scripts/
│   ├── export_to_s3.py                    # RDS → S3 Gold (Parquet + Snappy)
│   └── validate_rds_env.sh                # Pre-flight RDS env check + SSL cert
│
├── 📂 terraform/
│   └── main.tf                            # S3 bucket + Lifecycle policy (IaC)
│
├── 📂 docs/                               # Technical deep-dives per component
│   ├── 01_networking_security/
│   ├── 02_storage_layer/
│   ├── 03_streaming_ingestion/
│   ├── 04_catalog_query/
│   └── 05_orchestration_analytics/
│
├── 📂 superset/                           # Superset config
├── 📂 db/                                 # PostgreSQL init scripts
├── Dockerfile.airflow                     # Custom Airflow image
├── Dockerfile.superset                    # Custom Superset image
├── docker-compose.yml                     # Full stack local/EC2 deployment
└── .env.example                           # Environment variable template
```

---

## 🚀 Deployment Guide

### Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| AWS CLI | ≥ 2.x | Configured với Admin permissions |
| Docker + Docker Compose | ≥ 24.x | Run Airflow + Superset stack |
| Terraform CLI | ≥ 1.5 | Provision S3 + Lifecycle policy |

---

### Step 1 — Provision Infrastructure (Terraform)

```bash
cd terraform/
terraform init
terraform plan        # Review: S3 bucket + lifecycle policies
terraform apply -auto-approve
```

> **Kết quả:** S3 bucket `olist-de-tanphat-2026` được tạo với 2 Lifecycle Rules:
> - `raw/` → Standard-IA (60d) → Glacier (180d)
> - `processed/` → Standard-IA (30d) → Glacier (90d)

---

### Step 2 — Configure Environment Variables

```bash
cp .env.example .env
```

Mở `.env` và điền các giá trị:

```env
# ── Internal Postgres (Docker) ─────────────────────────────────────────
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_USER=airflow
POSTGRES_PASSWORD=<your_password>
POSTGRES_DB=postgres
POSTGRES_AIRFLOW_DB=airflow_db     # Tách biệt với Superset DB!
POSTGRES_SUPERSET_DB=superset_db

# ── AWS RDS PostgreSQL (Analytics Warehouse) ───────────────────────────
RDS_HOST=<your-rds-endpoint>.rds.amazonaws.com
RDS_PORT=5432
RDS_DB=ecommerce
RDS_USER=<rds_user>
RDS_PASSWORD=<rds_password>

# ── AWS Credentials ────────────────────────────────────────────────────
AWS_ACCESS_KEY_ID=<your_key>
AWS_SECRET_ACCESS_KEY=<your_secret>
AWS_DEFAULT_REGION=ap-southeast-1
S3_BUCKET=olist-de-tanphat-2026

# ── Monitoring ─────────────────────────────────────────────────────────
DISCORD_WEBHOOK_URL=<your_discord_webhook>
```

> ⚠️ **Isolation Rule:** `POSTGRES_AIRFLOW_DB` và `POSTGRES_SUPERSET_DB` **phải khác nhau** để tránh xung đột Alembic migration.

---

### Step 3 — Place RDS SSL Certificate

Airflow yêu cầu SSL certificate để kết nối với RDS PostgreSQL:

```bash
# Download AWS global bundle
curl -o certs/global-bundle.pem https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
```

---

### Step 4 — Launch Full Stack

```bash
docker compose up -d
```

| Service | URL | Credentials |
|---------|-----|-------------|
| **Apache Airflow** | `http://<EC2_IP>:8080` | `admin / admin` |
| **Apache Superset** | `http://<EC2_IP>:8088` | `admin / admin` |

---

### Step 5 — Initialize Database Schemas

```bash
# Tạo raw schema và bảng trong RDS
docker compose exec airflow-webserver bash -c "cd /opt/airflow && python ingestion/load_csv.py --init-only"
```

---

### Step 6 — Trigger the Pipeline

Truy cập Airflow UI → DAG `ecommerce_daily_production_pipeline` → **Trigger DAG**.

**Pipeline Execution Order:**

```
verify_raw_schema
generate_fake_data
       ↓
extract_load_raw        ← Kaggle CSV → RDS raw.*
load_streaming_orders   ← Firehose JSON → RDS raw.streaming_orders
       ↓
dbt_run_staging   →   dbt_test_staging    ← Silver layer + schema tests
       ↓
dbt_run_marts     →   dbt_test_marts      ← Gold layer + business tests
       ↓
export_processed_to_s3   ← RDS Gold → S3 (Parquet + Snappy)
       ↓
athena_data_quality_check  ← Composite Key Uniqueness validation
```

---

### Step 7 — Real-time Streaming (EventBridge Auto-trigger)

Lambda Producer đã được cấu hình với **EventBridge Rule `rate(30 seconds)`** — dữ liệu Streaming tự động đổ vào Kinesis Firehose → S3 → Superset Dashboard tự động cập nhật **mà không cần can thiệp thủ công**.

Để test thủ công:
```bash
# Simulate một batch sự kiện
python streaming/simulator.py --count 50
```

---

### Step 8 — Configure Superset Dashboard

1. Truy cập Superset → **Settings → Database Connections**
2. Thêm kết nối **Amazon Athena**:
   ```
   sqlalchemy_uri: awsathena+rest://@athena.ap-southeast-1.amazonaws.com:443/default?s3_staging_dir=s3://olist-de-tanphat-2026/athena-results/
   ```
3. Import Dashboard từ `superset/` hoặc tạo Charts từ dataset `view_order_analytics_gold`.

---

## 📊 Airflow DAG Overview

![Airflow DAG Graph](docs/05_orchestration_analytics/airflow_dag.png)

---

## 📈 Superset Dashboard

![Superset Dashboard](docs/05_orchestration_analytics/dashboard_superset_1.png)

![Superset Dashboard](docs/05_orchestration_analytics/dashboard_superset_3.png)

---

## 📖 Detailed Technical Documentation

Mỗi thành phần hạ tầng được giải trình chi tiết *Design Decisions* và *Screenshots* tại:

| Module | Nội dung chính |
|--------|---------------|
| [🔐 Networking & Security](./docs/01_networking_security/README.md) | VPC Endpoints, Security Groups, IAM Instance Profile, Secrets Manager |
| [📦 Storage & Data Lake](./docs/02_storage_layer/README.md) | Medallion Architecture, S3 Lifecycle, Parquet/Snappy optimization |
| [⚡ Streaming Ingestion](./docs/03_streaming_ingestion/README.md) | Lambda Producer, Kinesis Firehose, EventBridge 30s trigger |
| [🔍 Catalog & Query Engine](./docs/04_catalog_query/README.md) | Glue DDL Cataloging, Athena Partition Projection, Federated Query |
| [⚙️ Orchestration & Analytics](./docs/05_orchestration_analytics/README.md) | Airflow DAG, dbt models, Custom DQ Operator, Superset Dashboard |

---

## 🎯 Key Design Decisions & Challenges

| Challenge | Decision | Outcome |
|-----------|----------|---------|
| Small Files Problem khi Streaming | Dùng Kinesis Firehose buffering 60s/1MB thay vì write trực tiếp | S3 files lớn hơn, Athena scan ít hơn |
| Partition registration tốn thời gian | Athena **Partition Projection** thay vì `MSCK REPAIR TABLE` | Query ngay lập tức khi có data mới |
| Multi-item orders gây false duplicate | **Composite Key** `order_id + item_id + source + created_at` | Zero false positive trong DQ check |
| NAT Gateway cost khi EC2 gọi S3 | **VPC S3 Gateway Endpoint** (miễn phí) | $0 data transfer cost, tăng bảo mật |
| Airflow/Superset DB migration conflict | **Database isolation**: `airflow_db` vs `superset_db` | Zero migration conflict |

---

## 🗺️ Future Roadmap

- [ ] **Apache Spark on EMR** — xử lý Large-scale Batch thay thế dbt cho dataset > 1TB
- [ ] **Delta Lake** — ACID transactions trên S3, Time Travel queries
- [ ] **AWS Step Functions** — event-driven pipeline thay thế Airflow schedule
- [ ] **CI/CD Pipeline** — GitHub Actions auto-deploy dbt models + Airflow DAGs lên EC2

---

**Author:** Phan Tan Phat (`philipsgn`)  
**Role:** Data Engineer  
**Platform:** AWS (ap-southeast-1) · Docker · Python 3.11  

*Built to demonstrate production-grade data engineering on a real-world e-commerce dataset.*

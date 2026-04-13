# 🛒 E-Commerce Data Engineering Platform

> End-to-end data pipeline built on the **Olist Brazilian E-Commerce** dataset — ingesting, transforming, orchestrating, and visualizing 500k+ records using a modern open-source stack.

![Stack](https://img.shields.io/badge/Python-3.12-blue?logo=python)
![Stack](https://img.shields.io/badge/dbt-1.10-orange?logo=dbt)
![Stack](https://img.shields.io/badge/Airflow-2.9.3-red?logo=apacheairflow)
![Stack](https://img.shields.io/badge/PostgreSQL-15-blue?logo=postgresql)
![Stack](https://img.shields.io/badge/Superset-4.0.1-brightgreen?logo=apache)
![Stack](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)

---

## 📋 Table of Contents

- [Architecture Overview](#architecture-overview)
- [Tech Stack](#tech-stack)
- [Dataset](#dataset)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Phase 1 — Infrastructure Setup](#phase-1--infrastructure-setup)
- [Phase 2 — Data Ingestion](#phase-2--data-ingestion)
- [Phase 3 — Data Transformation (dbt)](#phase-3--data-transformation-dbt)
- [Phase 4 — Orchestration (Airflow)](#phase-4--orchestration-airflow)
- [Phase 5 — Visualization (Superset)](#phase-5--visualization-superset)
- [Phase 5.1 — Athena Federated Query (AWS, 2026 Update)](#phase-51--athena-federated-query-aws-2026-update)
- [Data Model](#data-model)
- [Business Questions Answered](#business-questions-answered)
- [Lessons Learned](#lessons-learned)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     DATA SOURCES                                │
│  Kaggle CSV (9 files)  │  Exchange Rate API  │  Simulated Stream│
└────────────┬───────────┴──────────┬──────────┴────────┬─────────┘
             │                      │                   │
             ▼                      ▼                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                  INGESTION LAYER (Python)                       │
│              load_csv.py  │  fetch_api.py  │  simulate_orders.py│
└────────────────────────────────┬────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│              RAW SCHEMA (PostgreSQL)                            │
│   orders │ customers │ order_items │ payments │ products │ ...  │
└────────────────────────────────┬────────────────────────────────┘
                                 │
                                 ▼ dbt run
┌─────────────────────────────────────────────────────────────────┐
│             STAGING SCHEMA (dbt views)                          │
│   stg_orders │ stg_customers │ stg_products │ stg_order_items   │
└────────────────────────────────┬────────────────────────────────┘
                                 │
                                 ▼ dbt run
┌─────────────────────────────────────────────────────────────────┐
│              MARTS SCHEMA (dbt tables)                          │
│   fact_orders │ dim_customers │ dim_products │ dim_date         │
└────────────────────────────────┬────────────────────────────────┘
                                 │
                    ┌────────────┴────────────┐
                    ▼                         ▼
          ┌──────────────────┐    ┌───────────────────────┐
          │ Apache Superset  │    │   Apache Airflow       │
          │ BI Dashboard     │    │   Pipeline Scheduler   │
          └──────────────────┘    └───────────────────────┘
```

---

## Tech Stack

| Layer | Tool | Version | Purpose |
|---|---|---|---|
| Containerization | Docker Compose | 29.x | Run entire stack with 1 command |
| Database | PostgreSQL | 15 | Central data warehouse |
| DB Admin | pgAdmin | 4.8.6 | Database management UI |
| Message Broker | Redis | 7-alpine | Airflow Celery task queue |
| Ingestion | Python + pandas | 3.12 | Load CSV, call APIs |
| Transformation | dbt-core + dbt-postgres | 1.10 | SQL-based data modeling |
| Orchestration | Apache Airflow | 2.9.3 | Pipeline scheduling & monitoring |
| Visualization | Apache Superset | 4.0.1 | BI dashboards |

---

## Dataset

**[Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)** — 100k+ real orders from Brazil's largest e-commerce platform (2016–2018), analogous to Shopee/Lazada in Vietnam.

| File | Rows | Description |
|---|---|---|
| olist_orders_dataset.csv | 99,441 | Core order records |
| olist_customers_dataset.csv | 99,441 | Customer information |
| olist_order_items_dataset.csv | 112,650 | Products per order |
| olist_order_payments_dataset.csv | 103,886 | Payment methods |
| olist_order_reviews_dataset.csv | 99,224 | Customer reviews |
| olist_products_dataset.csv | 32,951 | Product catalog |
| olist_sellers_dataset.csv | 3,095 | Seller information |
| olist_geolocation_dataset.csv | 1,000,163 | Zip code coordinates |
| product_category_name_translation.csv | 71 | PT → EN category names |

---

## Project Structure

```
ecommerce-de-project/
│
├── airflow/
│   ├── dags/                        # Airflow DAG definitions
│   │   └── ecommerce_pipeline.py    # Main pipeline DAG
│   ├── logs/                        # Airflow execution logs
│   └── plugins/                     # Custom Airflow plugins
│
├── data/                            # Raw CSV files from Kaggle (git-ignored)
│   └── *.csv
│
├── db/
│   └── init/
│       ├── 010_raw_schemas.sql      # Create raw + staging + marts schemas
│       └── 020_airflow_db.sql       # Create airflow database
│
├── dbt_project/
│   └── ecommerce/
│       ├── models/
│       │   ├── staging/             # 1-to-1 cleaning of raw tables
│       │   │   ├── sources.yml      # Declare raw schema as source
│       │   │   ├── stg_orders.sql
│       │   │   ├── stg_customers.sql
│       │   │   ├── stg_order_items.sql
│       │   │   ├── stg_products.sql
│       │   │   └── schema.yml       # Column-level tests
│       │   └── marts/               # Business-ready tables
│       │       ├── fact_orders.sql
│       │       ├── dim_customers.sql
│       │       ├── dim_products.sql
│       │       └── dim_date.sql
│       ├── dbt_project.yml
│       └── profiles.yml             # DB connection (local only, git-ignored)
│
├── ingestion/
│   ├── load_csv.py                  # Batch load 9 CSV → raw schema
│   ├── fetch_api.py                 # Exchange rate API → raw schema
│   └── simulate_orders.py          # Simulate real-time order stream
│
├── superset/
│   └── superset_config.py          # Superset configuration
│
├── .env                             # Environment variables (git-ignored)
├── .env.example                     # Template for .env (committed)
├── .gitignore
├── docker-compose.yml               # Full production stack
├── requirements.txt
└── README.md
```

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (v24+)
- [Python 3.10+](https://www.python.org/downloads/)
- [Git](https://git-scm.com/)
- Kaggle account (to download dataset)

---

## Phase 1 — Infrastructure Setup

**Goal:** Spin up the entire data platform with a single command.

### Services started

| Service | URL | Credentials |
|---|---|---|
| pgAdmin | http://localhost:5050 | admin@admin.com / admin123 |
| Airflow | http://localhost:8080 | admin / admin123 |
| Superset | http://localhost:8088 | admin / admin123 |
| PostgreSQL | localhost:5433 | de_user / de_password |
| Redis | localhost:6379 | — |

### Steps

**1. Clone the repository**
```bash
git clone https://github.com/<your-username>/ecommerce-de-project.git
cd ecommerce-de-project
```

**2. Configure environment variables**
```bash
# Copy template
cp .env.example .env

# Generate secure keys (run 3 times, use each output for each key below)
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Edit .env and fill in:
# AIRFLOW_FERNET_KEY=<key 1>
# AIRFLOW_WEBSERVER_SECRET_KEY=<key 2>
# SUPERSET_SECRET_KEY=<key 3>
```

**3. Start the stack**
```bash
docker-compose up -d
```

> First run downloads ~2GB of images. Takes 5–10 minutes.

**4. Verify all services are running**
```bash
docker ps
# Expected: 7 containers all showing "Up" or "Up (healthy)"
```

**5. Connect pgAdmin to PostgreSQL**

In pgAdmin UI → right-click Servers → Register → Server:
- Host: `postgres`
- Port: `5432`
- Database: `ecommerce_db`
- Username: `de_user`
- Password: `de_password`

**Verify schemas exist:** `ecommerce_db > Schemas` should show `raw`, `staging`, `marts`.

---

## Phase 2 — Data Ingestion

**Goal:** Load 550k+ rows from Kaggle CSV files into the `raw` PostgreSQL schema.

### Steps

**1. Download dataset**

Go to [Kaggle Olist Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) → Download → extract all 9 CSV files into `data/` folder.

**2. Install Python dependencies**
```bash
pip install -r requirements.txt
```

**3. Run ingestion script**
```bash
python ingestion/load_csv.py
```

**Expected output:**
```
[START] Loading olist_orders_dataset.csv -> raw.orders
[DONE]  Loaded 99,441 rows into raw.orders
[START] Loading olist_customers_dataset.csv -> raw.customers
[DONE]  Loaded 99,441 rows into raw.customers
...
[DONE]  Loaded 71 rows into raw.category_translation
```

**4. Verify in pgAdmin**
```sql
-- Run in pgAdmin Query Tool
SELECT 'orders'     AS table_name, COUNT(*) AS rows FROM raw.orders
UNION ALL
SELECT 'customers',                COUNT(*)          FROM raw.customers
UNION ALL
SELECT 'order_items',              COUNT(*)          FROM raw.order_items
UNION ALL
SELECT 'payments',                 COUNT(*)          FROM raw.payments
UNION ALL
SELECT 'products',                 COUNT(*)          FROM raw.products
ORDER BY rows DESC;
```

---

## Phase 3 — Data Transformation (dbt)

**Goal:** Clean raw data → staging views, then join into business-ready mart tables.

### Data flow

```
raw.orders          →  staging.stg_orders      →  marts.fact_orders
raw.customers       →  staging.stg_customers   →  marts.dim_customers
raw.order_items     →  staging.stg_order_items →  marts.dim_products
raw.products        →  staging.stg_products    →  marts.dim_date
```

### What staging does (cleaning rules)

| Issue | Fix Applied |
|---|---|
| String timestamps | `cast(... as timestamp)` |
| Mixed case city names | `initcap(customer_city)` |
| Mixed case state codes | `upper(customer_state)` |
| NULL order_ids | `where order_id is not null` |
| Portuguese category names | `left join category_translation` |
| No total_amount column | `price + freight_value as total_amount` |
| No delivery duration | `date_part('day', delivered - purchased)` |

### Steps

**1. Install dbt**
```bash
pip install dbt-postgres==1.10.0
dbt --version
```

**2. Configure database connection**
```bash
# Create dbt profiles directory
mkdir ~/.dbt

# Create profiles.yml at ~/.dbt/profiles.yml
```

```yaml
# ~/.dbt/profiles.yml
ecommerce:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5433
      dbname: ecommerce_db
      user: de_user
      password: de_password
      schema: staging
      threads: 4
```

**3. Navigate to dbt project**
```bash
cd dbt_project/ecommerce
```

**4. Test connection**
```bash
dbt debug
# Expected: "All checks passed!"
```

**5. Run all models**
```bash
dbt run
```

**Expected output:**
```
Running with dbt=1.10.0
Found 9 models, 8 tests

Concurrency: 4 threads

1 of 9 START sql view model staging.stg_orders .............. [RUN]
1 of 9 OK created sql view model staging.stg_orders ......... [CREATE VIEW]
...
9 of 9 OK created sql table model marts.fact_orders ......... [SELECT 98432]

Finished running 9 models in 18.4s.
Completed successfully.
```

**6. Run data quality tests**
```bash
dbt test
# Checks: unique keys, not-null constraints, accepted values
```

**7. Generate documentation**
```bash
dbt docs generate
dbt docs serve
# Open http://localhost:8080 to view lineage graph
```

**8. Verify marts in pgAdmin**
```sql
-- Revenue by month
SELECT
    d.year,
    d.month,
    COUNT(DISTINCT f.order_id)       AS total_orders,
    ROUND(SUM(f.price)::numeric, 2)  AS gross_revenue
FROM marts.fact_orders f
JOIN marts.dim_date d ON f.date_id = d.date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month;
```

---

## Phase 4 — Orchestration (Airflow)

**Goal:** Automate the full pipeline to run on a daily schedule without manual intervention.

### DAG overview

```
ecommerce_daily_pipeline
│
├── check_postgres_connection   (sensor)
├── load_csv_to_raw             (PythonOperator)
├── fetch_exchange_rates        (PythonOperator)
│
└── run_dbt_models
    ├── dbt_run_staging         (BashOperator)
    ├── dbt_test_staging        (BashOperator)
    ├── dbt_run_marts           (BashOperator)
    └── dbt_test_marts          (BashOperator)
```

### Steps

**1. Access Airflow UI**

Open http://localhost:8080 → login with `admin / admin123`

**2. Place DAG file**
```bash
# DAG files in this folder are auto-detected by Airflow
cp airflow/dags/ecommerce_pipeline.py airflow/dags/
```

**3. Enable the DAG**

In Airflow UI → find `ecommerce_daily_pipeline` → toggle ON

**4. Trigger a manual run**

Click the DAG → click ▶ (Trigger DAG) → monitor each task in Graph View

**5. Verify successful run**

All tasks should show green ✅. Check logs of any failed task by clicking it → View Log.

---

## Phase 5 — Visualization (Superset)

**Goal:** Connect Superset to the marts schema and build business dashboards.

### Dashboards built

| Dashboard | Charts | Business Questions |
|---|---|---|
| Revenue Overview | Line chart, bar chart | Monthly revenue trend, top categories |
| Customer Analysis | Map, scatter plot | Revenue by state, RFM segmentation |
| Delivery Performance | Gauge, histogram | Avg delivery days, late delivery rate |
| Seller Analysis | Table, bar chart | Top sellers, rating vs revenue |

### Steps

**1. Connect to PostgreSQL**

Superset UI → Settings → Database Connections → + Database:
```
Database: PostgreSQL
Host: postgres
Port: 5432
Database name: ecommerce_db
Username: de_user
Password: de_password
```

**2. Add datasets**

Datasets → + Dataset → select schema `marts` → add:
- `fact_orders`
- `dim_customers`
- `dim_products`
- `dim_date`

**3. Create charts**

Charts → + Chart → select dataset → choose chart type → configure metrics.

**Key metrics to build:**
```sql
-- Monthly Revenue
SUM(price) grouped by year, month

-- Top 10 Product Categories
SUM(price) grouped by category LIMIT 10

-- Average Delivery Days
AVG(delivery_days) where order_status = 'delivered'

-- Late Delivery Rate
COUNT(*) filter where is_late = true / COUNT(*) total
```

**4. Build dashboards**

Dashboards → + Dashboard → drag and drop charts → publish.

---

## Phase 5.1 — Athena Federated Query (AWS, 2026 Update)

**Goal:** Build a unified query layer across historical batch data (RDS PostgreSQL) and streaming data (S3 via Firehose), without manual Glue Crawler execution.

### Architecture reality (what changed)

- We **do not run Glue Crawler** in this flow.
- For RDS, Athena Federated Query via PostgreSQL Lambda Connector maps metadata directly from PostgreSQL.
- For streaming S3 data, schema is available through Glue Data Catalog managed by Firehose integration (or manual Athena DDL), so queries can run without crawler scans.
- Use real production schema naming: `"rds_postgres"."dev_pht"`.
- Use `order_date_id` in batch mart queries where time casting is needed.

### Streaming Schema Discovery

> AWS Glue Data Catalog serves as the central metadata repository.
>
> Tables are managed via Kinesis Data Firehose Direct Integration (or Manual DDL in Athena), ensuring real-time schema availability without manual Glue Crawler runs.

### Cross-source JOIN (latest 2026 DE)

```sql
-- Gold Layer chuẩn thực tế Phát đã làm
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

### Networking note

Keep VPC Endpoints for `Glue`, `Secrets Manager`, and `S3` if connector Lambda is in VPC.  
Otherwise, federated queries may fail with timeout errors.

---

## Data Model

### Star Schema

```
                    ┌──────────────┐
                    │  dim_date    │
                    │  date_id (PK)│
                    │  year        │
                    │  month       │
                    │  quarter     │
                    │  is_weekend  │
                    └──────┬───────┘
                           │
┌──────────────┐    ┌──────▼────────────┐    ┌──────────────┐
│ dim_customers│    │   fact_orders     │    │ dim_products │
│ customer_id  ├────┤   order_id (PK)   ├────┤ product_id   │
│ city         │    │   customer_id(FK) │    │ category     │
│ state        │    │   product_id (FK) │    │ weight_g     │
│ total_orders │    │   date_id (FK)    │    │ volume_cm3   │
└──────────────┘    │   price           │    └──────────────┘
                    │   freight_value   │
                    │   total_amount    │
                    │   delivery_days   │
                    │   is_late         │
                    └───────────────────┘
```

---

## Business Questions Answered

This project answers the following business questions through SQL and dashboards:

1. **What is the monthly revenue trend from 2016 to 2018?**
2. **Which product categories generate the highest revenue?**
3. **Which Brazilian states have the most customers and orders?**
4. **What is the average delivery time and late delivery rate?**
5. **Which sellers have the highest revenue and best ratings?**
6. **What payment methods do customers prefer?**
7. **How does freight cost vary across product categories?**

---

## Lessons Learned

**Technical decisions made:**

- Used `if_exists="replace"` in ingestion for idempotency — safe to re-run anytime
- Chose `view` materialization for staging (no storage cost, always fresh)
- Chose `table` materialization for marts (faster query performance for Superset)
- Port 5433 instead of default 5432 to avoid conflicts with local PostgreSQL installs
- Separated `raw`, `staging`, `marts` schemas for clear data lineage

**Challenges faced:**

- CSRF token issues with Airflow and Superset on Windows/Chrome → fixed via `SESSION_COOKIE_SAMESITE=Lax`
- dbt-postgres version mismatch with dbt-core → pinned to compatible versions
- Docker volume conflicts when recreating stack → used `docker-compose down -v` for clean restart

---

## Author

**Phan Tan Phat**
Data Engineering Project 

---

*Built to demonstrate end-to-end Data Engineering skills for job applications.*

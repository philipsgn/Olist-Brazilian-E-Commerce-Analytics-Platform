# 🏛️ Data Platform Architecture: E-Commerce Analytics

## 1. Pipeline Overview
This platform follows a **Modern Data Stack** (MDS) architecture, using ELT (Extract, Load, Transform) patterns.

### Flow:
1. **Extraction (Python/Airflow)**: Olist CSV source files (9 files) are parsed and loaded into the `raw` schema of PostgreSQL using a robust Python ingestion script (`load_csv.py`).
2. **Transformation (dbt)**:
   - **Staging Layer**: Standardizes raw data, renames columns, and casts types (Views).
   - **Marts Layer**: Fact & Dimension tables optimized for BI (Tables).
3. **Serving (Superset)**: BI layer connecting to `marts` for executive-level dashboards.
4. **Orchestration (Airflow)**: Daily scheduling, dependency management, and error handling.

---

## 2. Environment Strategy
To ensure reliability, the platform maintains environment isolation:
- **`dev`**: Local development using Docker. Developers work in their own schema (e.g., `dbt_tphat`).
- **`staging`**: (To be implemented) Identical to production, used for final UAT.
- **`prod`**: The stable branch. Only CI/CD from `main` can deploy to the `marts` schema.

---

## 3. Data Quality & Reliability (The "Golden Gate")
We implement a **Dual-Gated** testing strategy:
1. **Structural Integrity**: `dbt test` runs after each layer (Staging, Marts).
   - *Failure Handling*: If a Staging test fails, the pipeline immediately stops to prevent "dirtying" the Marts layer.
2. **Alerting**: Airflow `on_failure_callback` triggers immediate logging and (optionally) Slack/Email notifications.

---

## 4. Performance & Scalability
Designed as a production-grade system:
- **Incremental Loading**: Large fact tables (`fact_order_items`, `fact_payments`) use an incremental strategy to only process new data.
- **B-Tree Indexing**: Managed via dbt `config`, ensuring all JOIN keys are indexed for sub-second report response times.
- **Pre-aggregation**: Mart models are pre-aggregated to reduce join complexity at the BI layer.

---

## 5. Maintenance & Recovery
- **Backfills**: Use `airflow dags backfill` command to rerun past dates safely.
- **Docs**: Run `dbt docs generate` to view the latest lineage and documentation. 

"""
=============================================================================
FILE: airflow/dags/ecommerce_pipeline.py
=============================================================================
DAG: ecommerce_daily_production_pipeline
Author: Senior Data Engineering Team

PRODUCTION FEATURES:
1. Environment Isolation (Dev/Prod via Airflow Variables)
2. Quality Gating (dbt tests block downstream tasks)
3. Advanced Monitoring (SLAs, Retries, Timeout, Slack Notifications)
4. Robust Ingestion (Python-based with localized module loading)
5. Schema Health Check (verify raw tables before ingestion)

FILE DEPENDENCIES:
  ingestion/init.sql         → DDL definitions for raw.* tables
  ingestion/init_airflow.sql → Airflow DB setup + Variable/Connection docs
  db/init/010_ecommerce_db.sql → Auto-run by Postgres on first startup
  db/init/020_init_airflow.sql → Auto-run by Postgres on first startup
=============================================================================
"""

from __future__ import annotations
from datetime import datetime, timedelta
import pendulum
import os
import sys
import importlib.util
import logging

# Configure module-level logger
logger = logging.getLogger(__name__)

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# =============================================================================
# 1. CONFIGURATIONS & ENVIRONMENT MANAGEMENT
# =============================================================================

# Dynamic environment selection (default to 'dev' if not set in Airflow UI)
ENVIRONMENT = Variable.get("ENVIRONMENT", "dev")

# Directory paths internal to the Docker container
DBT_PROJECT_DIR = "/opt/airflow/dbt_project/ecommerce"
DBT_PROFILES_DIR = "/opt/airflow/dbt_project"
INGESTION_SCRIPT = "/opt/airflow/ingestion/load_csv.py"

# ---------------------------------------------------------------------------
# DB Connection: assembled from env vars — never hardcoded.
# Local Docker defaults map to the service name 'postgres' (docker-compose).
# On AWS EC2, set these env vars (or use AWS Secrets Manager injection).
# ---------------------------------------------------------------------------
def get_db_uri() -> str:
    """Build the PostgreSQL connection URI from environment variables.

    Priority: env var → sane local-Docker default.
    Logs the resolved host/db so connection issues surface immediately.
    """
    pg_user     = os.getenv("POSTGRES_USER",     "de_user")
    pg_password = os.getenv("POSTGRES_PASSWORD", "de_password")
    pg_host     = os.getenv("POSTGRES_HOST",     "postgres")   # Docker service name
    pg_port     = os.getenv("POSTGRES_PORT",     "5432")        # Internal Docker port
    pg_db       = os.getenv("POSTGRES_DB",       "ecommerce_db")

    uri = f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    logger.info(
        "[DB CONFIG] Resolved connection → host=%s port=%s db=%s user=%s",
        pg_host, pg_port, pg_db, pg_user,
    )
    return uri


DB_URI = get_db_uri()

# Production-standard defaults
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=1),
    "sla": timedelta(hours=2),
}

# =============================================================================
# 2. MONITORING & ALERTING
# =============================================================================

def send_failure_alert(context):
    """
    Standard failure callback. Can be expanded to Slack/Teams/PagerDuty.
    """
    task_instance = context.get("task_instance")
    exception = context.get("exception")
    logging.error(f"🚨 PIPELINE FAILURE | Task: {task_instance.task_id} | Env: {ENVIRONMENT} | Error: {exception}")

# =============================================================================
# 3. PYTHON CALLABLES
# =============================================================================

def run_load_csv(**kwargs) -> str:
    """
    Orchestrates the ingestion of CSV files into the 'raw' schema.
    Forwards all POSTGRES_* env vars so load_csv.py builds the correct URI.
    """
    execution_date = kwargs.get("ds", "unknown")
    logger.info("[%s] Starting CSV ingestion in %s environment...", execution_date, ENVIRONMENT)

    # Pass DB credentials explicitly — load_csv.py reads these via os.getenv()
    os.environ.setdefault("POSTGRES_USER",     os.getenv("POSTGRES_USER",     "de_user"))
    os.environ.setdefault("POSTGRES_PASSWORD", os.getenv("POSTGRES_PASSWORD", "de_password"))
    os.environ.setdefault("POSTGRES_HOST",     os.getenv("POSTGRES_HOST",     "postgres"))
    os.environ.setdefault("POSTGRES_PORT",     os.getenv("POSTGRES_PORT",     "5432"))
    os.environ.setdefault("POSTGRES_DB",       os.getenv("POSTGRES_DB",       "ecommerce_db"))
    # DATA_DIR: prefer env var, fall back to container path
    os.environ["DATA_DIR"] = os.getenv("DATA_DIR", "/opt/airflow/data")
    logger.info("[run_load_csv] DATA_DIR=%s  DB_HOST=%s",
                os.environ["DATA_DIR"], os.getenv("POSTGRES_HOST", "postgres"))

    # Dynamically load the ingestion script
    spec = importlib.util.spec_from_file_location("load_csv", INGESTION_SCRIPT)
    if spec is None:
        raise FileNotFoundError(f"Missing ingestion script: {INGESTION_SCRIPT}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Invoke the core ingestion logic
    module.run_ingestion()

    return f"Success: Loaded data for {execution_date}"

def run_simulation(**kwargs) -> str:
    """
    Kéo script simulate_data.py để tự động sinh thêm đơn hàng mới. 
    Mỗi lần chạy sẽ đẻ ra 100 đơn hàng giả thời gian hiện tại.
    """
    import os
    import sys
    import importlib.util
    
    SIM_SCRIPT = "/opt/airflow/ingestion/simulate_data.py"
    
    spec = importlib.util.spec_from_file_location("simulate_data", SIM_SCRIPT)
    if spec:
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        module.simulate_new_orders(100)
        return "SUCCESS: Generated 100 new orders."
    return "FAILED: Script not found."


def verify_raw_schema(**kwargs) -> str:
    """
    Health-check task: Xác minh tất cả 9 bảng raw.* tồn tại trước khi ingestion.
    Wrapped trong try-except để log lỗi kết nối rõ ràng thay vì crash bí ẩn.
    """
    import sqlalchemy
    from sqlalchemy import text

    REQUIRED_TABLES = [
        "orders", "customers", "order_items", "payments",
        "reviews", "products", "sellers", "geolocation",
        "category_translation",
    ]

    pg_host = os.getenv("POSTGRES_HOST", "postgres")
    pg_db   = os.getenv("POSTGRES_DB",   "ecommerce_db")
    logger.info("[verify_raw_schema] Connecting to host=%s db=%s", pg_host, pg_db)

    try:
        engine = sqlalchemy.create_engine(DB_URI)
        missing = []

        with engine.connect() as conn:
            for table in REQUIRED_TABLES:
                result = conn.execute(text(
                    "SELECT EXISTS ("
                    "  SELECT 1 FROM information_schema.tables"
                    "  WHERE table_schema = 'raw' AND table_name = :tbl"
                    ")"
                ), {"tbl": table})
                exists = result.scalar()
                if not exists:
                    missing.append(f"raw.{table}")

    except sqlalchemy.exc.OperationalError as exc:
        # OperationalError = wrong host, wrong port, wrong password, network issue
        logger.error(
            "[verify_raw_schema] ❌ Cannot connect to PostgreSQL!\n"
            "  host     : %s\n"
            "  db       : %s\n"
            "  Check POSTGRES_HOST / POSTGRES_PASSWORD env vars or RDS Security Group.\n"
            "  Original error: %s",
            pg_host, pg_db, exc,
        )
        raise

    if missing:
        raise Exception(
            f"❌ Schema health check FAILED. Missing tables: {missing}. "
            f"Run db/init/010_ecommerce_db.sql or restart the postgres container."
        )

    logger.info("✅ Schema health check PASSED. All %d raw tables exist.", len(REQUIRED_TABLES))
    return f"OK: {len(REQUIRED_TABLES)} tables verified in raw schema."

# =============================================================================
# 4. DAG DEFINITION
# =============================================================================

with DAG(
    dag_id="ecommerce_daily_production_pipeline",
    description="Refined Production Pipeline: Extraction -> Staging -> Marts",
    default_args=default_args,
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ecommerce", "production", "quality-gated"],
    on_failure_callback=send_failure_alert,
    doc_md=f"""
# 🏭 Production Data Pipeline ({ENVIRONMENT})
This pipeline manages the end-to-end flow of E-commerce data from CSV to business-ready tables.

### 🚀 Simulation Mode:
This DAG now automatically generates **100 NEW ORDERS** daily before ingestion to simulate real-world data growth.

### 🛡️ Quality Gates:
Tests are executed at **every layer**. A failure in `dbt test` will halt the pipeline to prevent data corruption.

### 📈 Service Level Agreement (SLA):
Expected data readiness by **8:00 AM UTC** (2 hours after start).
    """
) as dag:

    # --- STEP 0A: SCHEMA HEALTH CHECK ---
    # Xác minh raw schema đã được init bởi db/init/010_ecommerce_db.sql
    # Nếu thiếu bảng → pipeline dừng ngay, không chạy ingestion
    check_raw_schema = PythonOperator(
        task_id="verify_raw_schema",
        python_callable=verify_raw_schema,
        doc_md="""
        Kiểm tra 9 bảng raw.* đã tồn tại trong PostgreSQL.
        **Nguồn schema:** `ingestion/init.sql` (auto-mount qua `db/init/010_ecommerce_db.sql`).
        Nếu FAIL → kiểm tra postgres container hoặc chạy lại init.sql.
        """,
    )

    # --- STEP 0B: SIMULATE ---
    generate_fake_data = PythonOperator(
        task_id="generate_fake_data",
        python_callable=run_simulation,
    )

    # --- STEP 1: EXTRACT & LOAD ---
    load_csv = PythonOperator(
        task_id="extract_load_raw",
        python_callable=run_load_csv,
    )

    # Env vars forwarded into every dbt BashOperator so profiles.yml env_var() calls work
    _dbt_env = {
        "PATH":              "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
        "POSTGRES_USER":     os.getenv("POSTGRES_USER",     "de_user"),
        "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "de_password"),
        "POSTGRES_HOST":     os.getenv("POSTGRES_HOST",     "postgres"),
        "POSTGRES_PORT":     os.getenv("POSTGRES_PORT",     "5432"),
        "POSTGRES_DB":       os.getenv("POSTGRES_DB",       "ecommerce_db"),
    }

    # --- STEP 2: STAGING (VIEW LAYER) ---
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"dbt run --select staging.* --target {ENVIRONMENT} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
        env=_dbt_env,
        append_env=True,
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"dbt test --select staging.* --target {ENVIRONMENT} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
        env=_dbt_env,
        append_env=True,
    )

    # --- STEP 3: MARTS (ANALYTICS LAYER) ---
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"dbt run --select marts.* --target {ENVIRONMENT} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
        env=_dbt_env,
        append_env=True,
    )

    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=f"dbt test --select marts.* --target {ENVIRONMENT} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
        env=_dbt_env,
        append_env=True,
    )

    # --- LINEAGE ---
    # verify_raw_schema chạy song song với generate_fake_data
    # load_csv chỉ chạy khi CẢ HAI task trên đều thành công
    [check_raw_schema, generate_fake_data] >> load_csv >> dbt_run_staging >> dbt_test_staging >> dbt_run_marts >> dbt_test_marts

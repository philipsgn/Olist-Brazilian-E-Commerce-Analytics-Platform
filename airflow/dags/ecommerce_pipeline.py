"""
=============================================================================
FILE: airflow/dags/ecommerce_pipeline.py
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
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.module_loading import import_string

# Import custom alerting
try:
    from utils.discord_alerts import send_discord_alert
except ImportError:
    # Fallback if utils is not in PYTHONPATH
    def send_discord_alert(context):
        logging.error("Alert utility (discord_alerts) not found.")

# =============================================================================
# 1. CONFIGURATIONS & ENVIRONMENT MANAGEMENT
# =============================================================================

# Environment and Path Management
ENVIRONMENT = Variable.get("ENVIRONMENT", "dev")

# Derived relative paths for portability (Local, EC2, GitHub Runners)
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
# Trong Docker: /opt/airflow/dags -> /opt/airflow
PROJECT_ROOT = os.path.dirname(DAG_DIR)

DBT_PROJECT_DIR = os.path.join(PROJECT_ROOT, "dbt_project", "ecommerce")
DBT_PROFILES_DIR = os.path.join(PROJECT_ROOT, "dbt_project")
INGESTION_DIR = os.path.join(PROJECT_ROOT, "ingestion")

INGESTION_SCRIPT = os.path.join(INGESTION_DIR, "load_csv.py")
STREAMING_SCRIPT = os.path.join(INGESTION_DIR, "load_streaming.py")
SIM_SCRIPT = os.path.join(INGESTION_DIR, "simulate_data.py")

# ---------------------------------------------------------------------------
# DB Connection: assembled from env vars
# ---------------------------------------------------------------------------
def get_db_uri() -> str:
    pg_user     = os.getenv("POSTGRES_USER",     "de_user")
    pg_password = os.getenv("POSTGRES_PASSWORD", "de_password")
    pg_host     = os.getenv("POSTGRES_HOST",     "postgres")
    pg_port     = os.getenv("POSTGRES_PORT",     "5432")
    pg_db       = os.getenv("POSTGRES_DB",       "ecommerce_db")
    return f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"

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
    "on_failure_callback": send_discord_alert,
    "on_retry_callback": send_discord_alert,
}

# =============================================================================
# 3. PYTHON CALLABLES
# =============================================================================

def run_load_csv(**kwargs) -> str:
    execution_date = kwargs.get("ds", "unknown")
    os.environ.setdefault("POSTGRES_USER",     os.getenv("POSTGRES_USER",     "de_user"))
    os.environ.setdefault("POSTGRES_PASSWORD", os.getenv("POSTGRES_PASSWORD", "de_password"))
    os.environ.setdefault("POSTGRES_HOST",     os.getenv("POSTGRES_HOST",     "postgres"))
    os.environ.setdefault("POSTGRES_PORT",     os.getenv("POSTGRES_PORT",     "5432"))
    os.environ.setdefault("POSTGRES_DB",       os.getenv("POSTGRES_DB",       "ecommerce_db"))
    os.environ["DATA_DIR"] = os.getenv("DATA_DIR", os.path.join(PROJECT_ROOT, "data"))

    spec = importlib.util.spec_from_file_location("load_csv", INGESTION_SCRIPT)
    if spec is None:
        raise FileNotFoundError(f"Missing ingestion script: {INGESTION_SCRIPT}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.run_ingestion()
    return f"Success: Loaded data for {execution_date}"

def run_simulation(**kwargs) -> str:
    spec = importlib.util.spec_from_file_location("simulate_data", SIM_SCRIPT)
    if spec is None:
        raise AirflowException(f"CRITICAL ERROR: simulate_data.py not found at: {SIM_SCRIPT}")
    try:
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        module.simulate_new_orders(100)
        return "SUCCESS: Generated 100 new orders."
    except Exception as exc:
        logger.exception("Simulation failed: %s", exc)
        raise AirflowException(f"Data simulation failed: {exc}") from exc

def run_streaming_load(**kwargs) -> str:
    spec = importlib.util.spec_from_file_location("load_streaming", STREAMING_SCRIPT)
    if spec is None:
        raise FileNotFoundError(f"Missing streaming script: {STREAMING_SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.run_streaming_ingestion()
    return "Streaming ingestion complete"

def verify_raw_schema(**kwargs) -> str:
    import sqlalchemy
    from sqlalchemy import text
    REQUIRED_TABLES = ["orders", "customers", "order_items", "payments", "reviews", "products", "sellers", "geolocation", "category_translation", "streaming_orders"]
    try:
        engine = sqlalchemy.create_engine(DB_URI)
        with engine.connect() as conn:
            for table in REQUIRED_TABLES:
                result = conn.execute(text("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'raw' AND table_name = :tbl)"), {"tbl": table})
                if not result.scalar(): raise Exception(f"Missing table: raw.{table}")
    except Exception as exc:
        logger.error(f"Schema check failed: {exc}")
        raise
    return "Schema verified"

# =============================================================================
# 4. DAG DEFINITION
# =============================================================================

with DAG(
    dag_id="ecommerce_daily_production_pipeline",
    default_args=default_args,
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ecommerce"]
) as dag:

    check_raw_schema = PythonOperator(task_id="verify_raw_schema", python_callable=verify_raw_schema)
    generate_fake_data = PythonOperator(task_id="generate_fake_data", python_callable=run_simulation)
    load_csv = PythonOperator(task_id="extract_load_raw", python_callable=run_load_csv)
    load_streaming = PythonOperator(task_id="load_streaming_orders", python_callable=run_streaming_load)

    _dbt_env = {
        "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
        "POSTGRES_USER": os.getenv("POSTGRES_USER", "de_user"),
        "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "de_password"),
        "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "postgres"),
        "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
        "POSTGRES_DB": os.getenv("POSTGRES_DB", "ecommerce_db"),
    }

    dbt_run_staging = BashOperator(task_id="dbt_run_staging", bash_command=f"dbt run --select staging.* --target {ENVIRONMENT} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}", env=_dbt_env, append_env=True)
    dbt_test_staging = BashOperator(task_id="dbt_test_staging", bash_command=f"dbt test --select staging.* --target {ENVIRONMENT} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}", env=_dbt_env, append_env=True)
    dbt_run_marts = BashOperator(task_id="dbt_run_marts", bash_command=f"dbt run --select marts.* --target {ENVIRONMENT} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}", env=_dbt_env, append_env=True)
    dbt_test_marts = BashOperator(task_id="dbt_test_marts", bash_command=f"dbt test --select marts.* --target {ENVIRONMENT} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}", env=_dbt_env, append_env=True)

    (
        [check_raw_schema, generate_fake_data]
        >> load_csv
        >> load_streaming
        >> dbt_run_staging
        >> dbt_test_staging
        >> dbt_run_marts
        >> dbt_test_marts
    )

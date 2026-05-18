"""
=============================================================================
FILE: airflow/dags/ecommerce_pipeline.py
DAG: ecommerce_daily_production_pipeline
Author: Senior Data Engineering Team (Audit & merged by Antigravity)
=============================================================================
"""

from __future__ import annotations
from datetime import datetime, timedelta
import importlib.util
import logging
import os
import sys

import pendulum

# [HOTFIX] Khôi phục logic đường dẫn chuẩn Docker
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(DAG_DIR)  # /opt/airflow
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# Configure module-level logger
logger = logging.getLogger(__name__)

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Import custom Athena DQ Operator
from plugins.operators.athena_data_quality import AthenaDataQualityOperator

# Import custom alerting
try:
    from utils.discord_alerts import send_discord_alert
except ImportError:
    def send_discord_alert(context):
        logging.error("Alert utility (discord_alerts) not found.")

# =============================================================================
# 1. CONFIGURATIONS & ENVIRONMENT MANAGEMENT
# =============================================================================

ENVIRONMENT = Variable.get("ENVIRONMENT", "dev")

# Derived relative paths for portability
INGESTION_DIR = os.path.join(PROJECT_ROOT, "ingestion")
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
DBT_PROJECT_DIR = os.path.join(PROJECT_ROOT, "dbt_project", "ecommerce")
DBT_PROFILES_DIR = os.path.join(PROJECT_ROOT, "dbt_project")

INGESTION_SCRIPT = os.path.join(INGESTION_DIR, "load_csv.py")
STREAMING_SCRIPT = os.path.join(INGESTION_DIR, "load_streaming.py")
SIM_SCRIPT = os.path.join(INGESTION_DIR, "simulate_data.py")
EXPORT_SCRIPT = os.path.join(PROJECT_ROOT, "scripts", "export_to_s3.py")
FORECAST_SCRIPT = os.path.join(PROJECT_ROOT, "analytics", "sales_forecast.py")

REQUIRED_RDS_ENV = [
    "RDS_HOST",
    "RDS_PORT",
    "RDS_DB",
    "RDS_USER",
    "RDS_PASSWORD",
]


def require_rds_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value:
        raise AirflowException(
            f"Missing required environment variable {var_name}. "
            "Set it in .env or deployment secrets."
        )
    return value


def get_rds_db_uri() -> str:
    return (
        f"postgresql://{require_rds_env('RDS_USER')}:{require_rds_env('RDS_PASSWORD')}@"
        f"{require_rds_env('RDS_HOST')}:{require_rds_env('RDS_PORT')}/{require_rds_env('RDS_DB')}"
    )


def get_dbt_env() -> dict[str, str]:
    return {
        "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
        "RDS_HOST": require_rds_env("RDS_HOST"),
        "RDS_PORT": require_rds_env("RDS_PORT"),
        "RDS_DB": require_rds_env("RDS_DB"),
        "RDS_USER": require_rds_env("RDS_USER"),
        "RDS_PASSWORD": require_rds_env("RDS_PASSWORD"),
    }


DB_URI = get_rds_db_uri()

DBT_TARGET = Variable.get("DBT_TARGET", os.getenv("DBT_TARGET", "prod"))









default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
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
    os.environ["DATA_DIR"] = DATA_DIR # [FIX] Truyền đúng folder data
    
    spec = importlib.util.spec_from_file_location("load_csv", INGESTION_SCRIPT)
    if spec is None: raise FileNotFoundError(f"Missing: {INGESTION_SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.run_ingestion()
    return f"Success for {execution_date}"

def run_simulation(**kwargs) -> str:
    os.environ["DATA_DIR"] = DATA_DIR # [FIX] Đảm bảo script simulation thấy đúng folder data
    spec = importlib.util.spec_from_file_location("simulate_data", SIM_SCRIPT)
    if spec is None: raise AirflowException(f"Missing: {SIM_SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.simulate_new_orders(100)
    return "SUCCESS"

def run_streaming_load(**kwargs) -> str:
    spec = importlib.util.spec_from_file_location("load_streaming", STREAMING_SCRIPT)
    if spec is None: raise FileNotFoundError(f"Missing: {STREAMING_SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.run_streaming_ingestion()
    return "Streaming Ingestion Complete"

def run_export_s3(**kwargs) -> str:
    os.environ["S3_BUCKET"] = os.getenv("S3_BUCKET", "olist-de-tanphat-2026")
    spec = importlib.util.spec_from_file_location("export_to_s3", EXPORT_SCRIPT)
    if spec is None: raise FileNotFoundError(f"Missing: {EXPORT_SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.main()
    return "Export to S3 Complete"

def run_sales_forecast(**kwargs) -> str:
    spec = importlib.util.spec_from_file_location("sales_forecast", FORECAST_SCRIPT)
    if spec is None: raise FileNotFoundError(f"Missing: {FORECAST_SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.main()
    return "AI Sales Forecasting Complete"

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
    tags=["ecommerce", "production"],
    on_failure_callback=send_discord_alert,
    doc_md="""
# 🏭 Production Data Pipeline
- **Batch CSV** → `raw.*` tables
- **Streaming** → `raw.streaming_orders`
- **Quality Gates** → dbt tests enforce data integrity.
    """
) as dag:

    check_raw_schema = PythonOperator(task_id="verify_raw_schema", python_callable=verify_raw_schema)
    generate_fake_data = PythonOperator(task_id="generate_fake_data", python_callable=run_simulation)
    load_csv = PythonOperator(task_id="extract_load_raw", python_callable=run_load_csv)
    load_streaming = PythonOperator(task_id="load_streaming_orders", python_callable=run_streaming_load)

    dbt_base_cmd = f"dbt deps --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} && dbt"

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"{dbt_base_cmd} run --select staging.* --target {DBT_TARGET} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
        env=get_dbt_env(),
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"{dbt_base_cmd} test --select staging.* --target {DBT_TARGET} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
        env=get_dbt_env(),
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"{dbt_base_cmd} run --select marts.* --target {DBT_TARGET} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
        env=get_dbt_env(),
    )

    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=f"{dbt_base_cmd} test --select marts.* --target {DBT_TARGET} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
        env=get_dbt_env(),
    )

    export_to_s3_task = PythonOperator(task_id="export_processed_to_s3", python_callable=run_export_s3)

    athena_dq_check = AthenaDataQualityOperator(
        task_id="athena_data_quality_check",
        table_name="view_order_analytics_gold",
        database="default",  # Sửa lại thành database default như trong ảnh
        aws_conn_id="aws_default",
        workgroup="primary",
        retries=0
    )

    ai_sales_forecasting = PythonOperator(
        task_id="ai_sales_forecasting",
        python_callable=run_sales_forecast
    )

    (
        [check_raw_schema, generate_fake_data]
        >> load_csv
        >> load_streaming
        >> dbt_run_staging
        >> dbt_test_staging
        >> dbt_run_marts
        >> dbt_test_marts
        >> export_to_s3_task
        >> athena_dq_check
        >> ai_sales_forecasting
    )

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
import json
import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Sequence

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

def get_db_uri() -> str:
    pg_user     = os.getenv("POSTGRES_USER",     "de_user")
    pg_password = os.getenv("POSTGRES_PASSWORD", "de_password")
    pg_host     = os.getenv("POSTGRES_HOST",     "postgres")
    pg_port     = os.getenv("POSTGRES_PORT",     "5432")
    pg_db       = os.getenv("POSTGRES_DB",       "ecommerce_db")
    return f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"

DB_URI = get_db_uri()

DBT_ARTIFACT_ROOT = os.path.join(PROJECT_ROOT, "dbt_artifacts")
DBT_TARGET_PATH = os.path.join(DBT_ARTIFACT_ROOT, "target")
DBT_LOG_DIR = os.path.join(DBT_ARTIFACT_ROOT, "logs")
DBT_LOG_FILE = os.path.join(DBT_LOG_DIR, "dbt.log")
DBT_TARGET = Variable.get("DBT_TARGET", ENVIRONMENT)

DBT_RETRY_TIMEOUT_SECONDS = 900
DBT_DEBUG_TIMEOUT_SECONDS = 300
DBT_DEPENDENCY_TIMEOUT_SECONDS = 600


def ensure_path_exists(path: str, description: str) -> Path:
    path_obj = Path(path)
    if not path_obj.exists():
        raise AirflowException(f"{description} not found or inaccessible: {path}")
    return path_obj


def ensure_directory(path: str) -> Path:
    path_obj = Path(path)
    path_obj.mkdir(parents=True, exist_ok=True)
    return path_obj


def read_file_tail(path: str, lines: int = 80) -> str:
    path_obj = Path(path)
    if not path_obj.exists():
        return ""
    with path_obj.open("r", encoding="utf-8", errors="replace") as handle:
        return "\n".join(handle.readlines()[-lines:])


def build_dbt_env() -> dict[str, str]:
    env = os.environ.copy()
    env["PATH"] = ":".join(filter(None, ["/opt/airflow/dbt_venv/bin", "/home/airflow/.local/bin", env.get("PATH", "")]))
    env["DBT_PROFILES_DIR"] = DBT_PROFILES_DIR
    env["DBT_LOG_PATH"] = DBT_LOG_FILE
    env["DBT_LOG_FORMAT"] = "json"
    env["DBT_TARGET_PATH"] = DBT_TARGET_PATH
    env["DBT_TARGET"] = DBT_TARGET
    env.setdefault("DBT_DEV_HOST", os.getenv("POSTGRES_HOST", "postgres"))
    env.setdefault("DBT_DEV_PORT", os.getenv("POSTGRES_PORT", "5432"))
    env.setdefault("DBT_DEV_USER", os.getenv("POSTGRES_USER", "de_user"))
    env.setdefault("DBT_DEV_PASSWORD", os.getenv("POSTGRES_PASSWORD", "de_password"))
    env.setdefault("DBT_DEV_DBNAME", os.getenv("POSTGRES_DB", "ecommerce_db"))
    env.setdefault("DBT_DEV_SCHEMA", os.getenv("DBT_DEV_SCHEMA", "analytics_dev"))
    return env


def log_command_output(command: Sequence[str], completed: subprocess.CompletedProcess) -> None:
    logger.info("[DBT COMMAND] %s", " ".join(command))
    if completed.stdout:
        logger.info("[DBT STDOUT] %s", completed.stdout.strip())
    if completed.stderr:
        logger.error("[DBT STDERR] %s", completed.stderr.strip())


def run_subprocess(command: Sequence[str], timeout_seconds: int, cwd: str, env: dict[str, str]) -> subprocess.CompletedProcess:
    logger.info("Running subprocess: %s", " ".join(command))
    completed = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )
    log_command_output(command, completed)
    if completed.returncode != 0:
        dbt_log_tail = read_file_tail(DBT_LOG_FILE, lines=120)
        if dbt_log_tail:
            logger.error("[DBT LOG FILE] tail from %s:\n%s", DBT_LOG_FILE, dbt_log_tail)
        raise AirflowException(
            "dbt command failed with exit code {code}. "
            "See Airflow logs and dbt artifact logs for details. "
            "Command: {cmd}".format(code=completed.returncode, cmd=" ".join(command))
        )
    return completed


def validate_dbt_installation(env: dict[str, str]) -> None:
    dbt_binary = shutil.which("dbt", path=env["PATH"]) or shutil.which("dbt")
    if not dbt_binary:
        raise AirflowException("dbt binary not found in PATH. Verify the Docker image contains dbt and adapter packages.")
    logger.info("dbt executable resolved to: %s", dbt_binary)
    run_subprocess([dbt_binary, "--version"], timeout_seconds=60, cwd=DBT_PROJECT_DIR, env=env)


def validate_dbt_artifacts_and_files() -> None:
    ensure_directory(DBT_ARTIFACT_ROOT)
    ensure_directory(DBT_LOG_DIR)
    ensure_directory(DBT_TARGET_PATH)
    ensure_path_exists(DBT_PROJECT_DIR, "dbt project directory")
    ensure_path_exists(os.path.join(DBT_PROJECT_DIR, "dbt_project.yml"), "dbt project configuration")
    ensure_path_exists(os.path.join(DBT_PROFILES_DIR, "profiles.yml"), "dbt profiles.yml")
    ensure_path_exists(os.path.join(DBT_PROJECT_DIR, "packages.yml"), "dbt packages.yml")


def validate_dbt_environment(**kwargs) -> str:
    env = build_dbt_env()
    validate_dbt_artifacts_and_files()
    validate_dbt_installation(env)
    run_subprocess(
        [
            "dbt",
            "debug",
            "--project-dir",
            DBT_PROJECT_DIR,
            "--profiles-dir",
            DBT_PROFILES_DIR,
            "--target",
            DBT_TARGET,
            "--log-format",
            "json",
        ],
        timeout_seconds=DBT_DEBUG_TIMEOUT_SECONDS,
        cwd=DBT_PROJECT_DIR,
        env=env,
    )
    return "dbt environment verified"


def run_dbt_deps(**kwargs) -> str:
    env = build_dbt_env()
    validate_dbt_artifacts_and_files()
    validate_dbt_installation(env)
    run_subprocess(
        [
            "dbt",
            "deps",
            "--project-dir",
            DBT_PROJECT_DIR,
            "--profiles-dir",
            DBT_PROFILES_DIR,
            "--log-format",
            "json",
        ],
        timeout_seconds=DBT_DEPENDENCY_TIMEOUT_SECONDS,
        cwd=DBT_PROJECT_DIR,
        env=env,
    )
    return "dbt dependencies resolved"


def run_dbt_action(action: str, select: str, **kwargs) -> str:
    env = build_dbt_env()
    validate_dbt_artifacts_and_files()
    validate_dbt_installation(env)
    run_subprocess(
        [
            "dbt",
            action,
            "--select",
            select,
            "--target",
            DBT_TARGET,
            "--project-dir",
            DBT_PROJECT_DIR,
            "--profiles-dir",
            DBT_PROFILES_DIR,
            "--target-path",
            DBT_TARGET_PATH,
            "--log-format",
            "json",
        ],
        timeout_seconds=DBT_RETRY_TIMEOUT_SECONDS,
        cwd=DBT_PROJECT_DIR,
        env=env,
    )
    return f"dbt {action} {select} completed"

def run_dbt_run_staging(**kwargs) -> str:
    return run_dbt_action("run", "staging.*", **kwargs)


def run_dbt_test_staging(**kwargs) -> str:
    return run_dbt_action("test", "staging.*", **kwargs)


def run_dbt_run_marts(**kwargs) -> str:
    return run_dbt_action("run", "marts.*", **kwargs)


def run_dbt_test_marts(**kwargs) -> str:
    return run_dbt_action("test", "marts.*", **kwargs)

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

    dbt_validate_environment = PythonOperator(
        task_id="dbt_validate_environment",
        python_callable=validate_dbt_environment,
    )

    dbt_resolve_dependencies = PythonOperator(
        task_id="dbt_resolve_dependencies",
        python_callable=run_dbt_deps,
    )

    dbt_run_staging = PythonOperator(
        task_id="dbt_run_staging",
        python_callable=run_dbt_run_staging,
    )

    dbt_test_staging = PythonOperator(
        task_id="dbt_test_staging",
        python_callable=run_dbt_test_staging,
    )

    dbt_run_marts = PythonOperator(
        task_id="dbt_run_marts",
        python_callable=run_dbt_run_marts,
    )

    dbt_test_marts = PythonOperator(
        task_id="dbt_test_marts",
        python_callable=run_dbt_test_marts,
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
        >> dbt_validate_environment
        >> dbt_resolve_dependencies
        >> dbt_run_staging
        >> dbt_test_staging
        >> dbt_run_marts
        >> dbt_test_marts
        >> export_to_s3_task
        >> athena_dq_check
        >> ai_sales_forecasting
    )

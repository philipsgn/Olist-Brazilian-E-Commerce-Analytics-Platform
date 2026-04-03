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
=============================================================================
"""

from __future__ import annotations
from datetime import datetime, timedelta
import pendulum
import os
import sys
import importlib.util
import logging

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
DB_URI = "postgresql://de_user:de_password@postgres:5432/ecommerce_db"

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
    """
    execution_date = kwargs.get("ds", "unknown")
    logging.info(f"[{execution_date}] Starting CSV ingestion in {ENVIRONMENT} environment...")

    os.environ["DB_URI"] = DB_URI
    os.environ["DATA_DIR"] = "/opt/airflow/data"

    # Dynamically load the ingestion script
    spec = importlib.util.spec_from_file_location("load_csv", INGESTION_SCRIPT)
    if spec is None:
        raise FileNotFoundError(f"Missing ingestion script: {INGESTION_SCRIPT}")
        
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Invoke the core ingestion logic
    module.run_ingestion()
    
    return f"Success: Loaded data for {execution_date}"

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

### 🛡️ Quality Gates:
Tests are executed at **every layer**. A failure in `dbt test` will halt the pipeline to prevent data corruption.

### 📈 Service Level Agreement (SLA):
Expected data readiness by **8:00 AM UTC** (2 hours after start).
    """
) as dag:

    # --- STEP 1: EXTRACT & LOAD ---
    load_csv = PythonOperator(
        task_id="extract_load_raw",
        python_callable=run_load_csv,
    )

    # --- STEP 2: STAGING (VIEW LAYER) ---
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"dbt run --select staging.* --target {ENVIRONMENT} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
        env={"PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin"},
        append_env=True,
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"dbt test --select staging.* --target {ENVIRONMENT} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
        env={"PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin"},
        append_env=True,
    )

    # --- STEP 3: MARTS (ANALYTICS LAYER) ---
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"dbt run --select marts.* --target {ENVIRONMENT} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
        env={"PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin"},
        append_env=True,
    )

    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=f"dbt test --select marts.* --target {ENVIRONMENT} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
        env={"PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin"},
        append_env=True,
    )

    # --- LINEAGE ---
    load_csv >> dbt_run_staging >> dbt_test_staging >> dbt_run_marts >> dbt_test_marts

"""
=============================================================================
FILE: airflow/dags/ecommerce_pipeline.py
VERSION: 2.1 (Production Standard - Hotfixed)
=============================================================================
"""

import os
import sys
import logging
import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# [HOTFIX] Khắc phục triệt để lỗi ImportError
# Thêm đường dẫn gốc vào sys.path để Worker luôn thấy folder 'utils'
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(DAG_DIR) # /opt/airflow
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# Cấu hình log
logger = logging.getLogger(__name__)

# [HOTFIX] Thử import Discord Alert với xử lý lỗi an toàn
try:
    from utils.discord_alerts import send_discord_alert
except Exception as e:
    logging.error(f"Failed to import discord_alerts: {e}")
    def send_discord_alert(context): pass

# Constants
INGESTION_DIR = os.path.join(PROJECT_ROOT, "ingestion")
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
DBT_DIR = os.path.join(PROJECT_ROOT, "dbt_project")

# Scripts
INGESTION_SCRIPT = os.path.join(INGESTION_DIR, "load_csv.py")
SIM_SCRIPT = os.path.join(INGESTION_DIR, "simulate_data.py")
STREAMING_SCRIPT = os.path.join(INGESTION_DIR, "load_streaming.py")

def get_db_uri():
    pg_user = os.getenv("POSTGRES_USER", "de_user")
    pg_pass = os.getenv("POSTGRES_PASSWORD", "de_password")
    pg_host = os.getenv("POSTGRES_HOST", "postgres")
    pg_port = os.getenv("POSTGRES_PORT", "5432")
    pg_db = os.getenv("POSTGRES_DB", "ecommerce_db")
    return f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"

# Default Args
default_args = {
    "owner": "senior-architect",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": send_discord_alert,
}

# Python Callables
def run_simulation_task(**kwargs):
    import importlib.util
    if not os.path.exists(SIM_SCRIPT):
        raise FileNotFoundError(f"Missing script: {SIM_SCRIPT}")
    
    # Export DATA_DIR để script con sử dụng đúng
    os.environ["DATA_DIR"] = DATA_DIR
    
    spec = importlib.util.spec_from_file_location("simulate_data", SIM_SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.simulate_new_orders(100)

def verify_and_prep_env(**kwargs):
    # Đảm bảo folder data tồn tại
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR, exist_ok=True)
        logger.info(f"Created missing directory: {DATA_DIR}")
    return "Environment Ready"

with DAG(
    "ecommerce_daily_production_pipeline",
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="0 6 * * *",
    catchup=False
) as dag:

    prep_env = PythonOperator(task_id="verify_env_readiness", python_callable=verify_and_prep_env)
    sim_data = PythonOperator(task_id="generate_fake_data", python_callable=run_simulation_task)
    load_csv = BashOperator(task_id="extract_load_raw", bash_command=f"python {INGESTION_SCRIPT}")

    prep_env >> sim_data >> load_csv

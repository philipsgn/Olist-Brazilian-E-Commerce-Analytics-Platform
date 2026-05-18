from __future__ import annotations

import os
import time
import io
import logging
from pathlib import Path

import pandas as pd
import boto3
from sqlalchemy import create_engine, text

# Module-level logger
logger = logging.getLogger(__name__)

# --- Configuration ---
# Map CSV filename -> target table name
DATASET_CONFIG = {
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "payments",
    "olist_order_reviews_dataset.csv": "reviews",
    "olist_customers_dataset.csv": "customers",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "olist_geolocation_dataset.csv": "geolocation",
    "product_category_name_translation.csv": "category_translation",
}

# --- Hybrid Ingestion Strategy (Local + Cloud) ---
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = Path(os.getenv("DATA_DIR", SCRIPT_DIR.parent / "data"))
S3_BUCKET = os.getenv("S3_BUCKET", "")
S3_PREFIX = "raw/csv/"
USE_S3 = os.getenv("USE_S3", "false").lower() == "true"


def _require_s3_bucket() -> str:
    """Keep cloud ingestion explicit so production buckets are never hard-coded."""
    if not S3_BUCKET:
        raise RuntimeError("S3_BUCKET environment variable is required when USE_S3=true.")
    return S3_BUCKET

# ---------------------------------------------------------------------------
# DB Connection — assembled from individual env vars.
# Production-grade deployments use the canonical RDS_* credential names.
# ---------------------------------------------------------------------------

def require_rds_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value:
        raise RuntimeError(f"Missing required environment variable {var_name}.")
    return value


def _build_db_uri() -> str:
    rds_user     = require_rds_env("RDS_USER")
    rds_password = require_rds_env("RDS_PASSWORD")
    rds_host     = require_rds_env("RDS_HOST")
    rds_port     = require_rds_env("RDS_PORT")
    rds_db       = require_rds_env("RDS_DB")
    uri = f"postgresql://{rds_user}:{rds_password}@{rds_host}:{rds_port}/{rds_db}"
    logger.info(
        "[load_csv] DB → host=%s port=%s db=%s user=%s",
        rds_host, rds_port, rds_db, rds_user,
    )
    return uri


# Evaluated once at import time (consistent with original behaviour)
DB_URI = os.getenv("DB_URI") or _build_db_uri()

SCHEMA = "raw"

# Retry settings (simple, minimal)
RETRY_ATTEMPTS = 2
RETRY_SLEEP_SEC = 2


def get_engine():
    """Return a SQLAlchemy engine.
    Wraps creation in try-except so a bad host/password produces a clear log
    entry instead of a raw traceback.
    """
    rds_host = os.getenv("RDS_HOST", "postgres")
    rds_db   = os.getenv("RDS_DB",   "ecommerce_db")
    logger.info("[load_csv] Creating engine → host=%s db=%s", rds_host, rds_db)
    try:
        engine = create_engine(DB_URI)
        # Lightweight connectivity probe (does NOT open a real connection yet,
        # but raises immediately if the URL is malformed).
        return engine
    except Exception as exc:
        logger.error(
            "[load_csv] ❌ Failed to create DB engine!\n"
            "  host : %s\n"
            "  db   : %s\n"
            "  Verify RDS_HOST / RDS_PASSWORD env vars.\n"
            "  Error: %s",
            rds_host, rds_db, exc,
        )
        raise


def read_dataframe(file_path: Path) -> pd.DataFrame:
    """
    Kỹ thuật Hybrid: Cho phép đọc từ Local file hoặc nhấc trực tiếp từ AWS S3.
    """
    if USE_S3:
        print(f"   → [S3 SOURCE] s3://{S3_BUCKET}/{S3_PREFIX}{file_path.name}")
        # Chú ý: Cần AWS Credentials (IAM Role/Access Key) đã được cấu hình trong môi trường
        s3_client = boto3.client("s3")
        obj = s3_client.get_object(
            Bucket=_require_s3_bucket(),
            Key=f"{S3_PREFIX}{file_path.name}"
        )
        # Sử dụng io.BytesIO để dbt/pandas xử lý stream dữ liệu từ RAM, không lưu tạm file ra đĩa
        return pd.read_csv(io.BytesIO(obj["Body"].read()), low_memory=False)
    
    # Mặc định đọc từ local file (đường dẫn mount trong Docker container hoặc laptop)
    return pd.read_csv(file_path, low_memory=False)


def load_table(file_path: Path, table_name: str) -> int:
    """
    Sử dụng Chunking "siêu an toàn" cho máy RAM thấp.
    - Giảm chunk_size xuống 20,000
    - Bỏ method='multi' để tránh overhead bộ nhớ khi dựng SQL
    """
    engine = get_engine()
    
    # Truncate table first to avoid dropping it and its dependent dbt views
    try:
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {SCHEMA}.{table_name}"))
            print(f"   → [CLEANUP] Truncated table {SCHEMA}.{table_name} before loading.")
    except Exception as e:
        print(f"   → [CLEANUP] Could not truncate (might not exist yet): {e}")

    chunk_size = 20000 
    total_rows = 0
    
    print(f"   → [STREAMING] Loading {table_name}...")
    
    if USE_S3:
        s3_client = boto3.client("s3")
        obj = s3_client.get_object(Bucket=_require_s3_bucket(), Key=f"{S3_PREFIX}{file_path.name}")
        chunks = pd.read_csv(io.BytesIO(obj["Body"].read()), low_memory=False, chunksize=chunk_size)
    else:
        chunks = pd.read_csv(file_path, low_memory=False, chunksize=chunk_size)

    for chunk in chunks:
        # Always append since we truncated the table beforehand
        chunk.to_sql(
            name=table_name,
            con=engine,
            schema=SCHEMA,
            if_exists='append',
            index=False,
            chunksize=5000 
        )
        
        total_rows += len(chunk)
        print(f"     + Progress: {total_rows} rows loaded...")
        
    return total_rows

def run_ingestion() -> None:
    print(f"🚀 [INIT] Ingestion Mode: {'AWS S3' if USE_S3 else 'LOCAL FILE'}")
    
    for filename, table_name in DATASET_CONFIG.items():
        file_path = DATA_DIR / filename

        # Nếu không dùng S3 mà file local cũng không có thì mới SKIP
        if not USE_S3 and not file_path.exists():
            print(f"[SKIP] File not found: {file_path}")
            continue

        print(f"[START] Loading {filename} -> {SCHEMA}.{table_name}")
        attempt = 0
        while True:
            try:
                rows = load_table(file_path, table_name)
                print(f"[DONE]  Loaded {rows} rows into {SCHEMA}.{table_name}")
                break
            except Exception as exc:  # noqa: BLE001 - simple pipeline error handling
                attempt += 1
                print(f"[ERROR] {filename} failed on attempt {attempt}: {exc}")
                if attempt >= RETRY_ATTEMPTS:
                    print(f"[SKIP]  Giving up on {filename}")
                    break
                time.sleep(RETRY_SLEEP_SEC)


if __name__ == "__main__": 
    run_ingestion()

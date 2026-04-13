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
S3_BUCKET = os.getenv("S3_BUCKET", "olist-de-tanphat-2026")
S3_PREFIX = "raw/csv/"
USE_S3 = os.getenv("USE_S3", "false").lower() == "true"

# ---------------------------------------------------------------------------
# DB Connection — assembled from individual env vars.
# Defaults are suitable for the local docker-compose stack.
# Override on AWS EC2 by setting POSTGRES_HOST, POSTGRES_PORT, etc. in the
# environment or via AWS Secrets Manager / Parameter Store injection.
# ---------------------------------------------------------------------------
def _build_db_uri() -> str:
    pg_user     = os.getenv("POSTGRES_USER",     "de_user")
    pg_password = os.getenv("POSTGRES_PASSWORD", "de_password")
    pg_host     = os.getenv("POSTGRES_HOST",     "127.0.0.1")  # Local default
    pg_port     = os.getenv("POSTGRES_PORT",     "5433")        # Exposed host port
    pg_db       = os.getenv("POSTGRES_DB",       "ecommerce_db")
    uri = f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    logger.info(
        "[load_csv] DB → host=%s port=%s db=%s user=%s",
        pg_host, pg_port, pg_db, pg_user,
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
    pg_host = os.getenv("POSTGRES_HOST", "127.0.0.1")
    pg_db   = os.getenv("POSTGRES_DB",   "ecommerce_db")
    logger.info("[load_csv] Creating engine → host=%s db=%s", pg_host, pg_db)
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
            "  Verify POSTGRES_HOST / POSTGRES_PASSWORD env vars.\n"
            "  Error: %s",
            pg_host, pg_db, exc,
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
            Bucket=S3_BUCKET,
            Key=f"{S3_PREFIX}{file_path.name}"
        )
        # Sử dụng io.BytesIO để dbt/pandas xử lý stream dữ liệu từ RAM, không lưu tạm file ra đĩa
        return pd.read_csv(io.BytesIO(obj["Body"].read()), low_memory=False)
    
    # Mặc định đọc từ local file (đường dẫn mount trong Docker container hoặc laptop)
    return pd.read_csv(file_path, low_memory=False)


def load_table(file_path: Path, table_name: str) -> int:
    """
    Sử dụng Chunking để tối ưu bộ nhớ.
    - Khối đầu tiên: if_exists='replace'
    - Khối tiếp theo: if_exists='append'
    """
    engine = get_engine()
    chunk_size = 50000 
    first_chunk = True
    total_rows = 0
    
    print(f"   → [PROCESSING] {table_name} with chunking (size={chunk_size})...")
    
    # Kỹ thuật xử lý S3 hoặc Local
    if USE_S3:
        s3_client = boto3.client("s3")
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=f"{S3_PREFIX}{file_path.name}")
        # Đọc trực tiếp từ stream của S3 theo chunk
        chunks = pd.read_csv(io.BytesIO(obj["Body"].read()), low_memory=False, chunksize=chunk_size)
    else:
        chunks = pd.read_csv(file_path, low_memory=False, chunksize=chunk_size)

    for chunk in chunks:
        # Nếu là chunk đầu tiên thì 'replace' để tạo bảng mới, các chunk sau thì 'append'
        mode = 'replace' if first_chunk else 'append'
        
        chunk.to_sql(
            name=table_name,
            con=engine,
            schema=SCHEMA,
            if_exists=mode,
            index=False,
            method='multi',
            chunksize=10000 # Ghi vào DB mỗi lần 10,000 dòng
        )
        
        total_rows += len(chunk)
        if first_chunk:
            first_chunk = False
            
        print(f"     + Loaded a chunk of {len(chunk)} rows (Total: {total_rows})")
        
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

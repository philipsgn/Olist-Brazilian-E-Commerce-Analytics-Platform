from __future__ import annotations

import os
import time
import io
from pathlib import Path

import pandas as pd
import boto3
from sqlalchemy import create_engine, text

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
DATA_DIR = Path("data")
S3_BUCKET = os.getenv("S3_BUCKET", "olist-de-tanphat-2026")
S3_PREFIX = "raw/csv/"
USE_S3 = os.getenv("USE_S3", "false").lower() == "true"

DB_URI = os.getenv(
    "DB_URI",
    "postgresql://de_user:de_password@127.0.0.1:5433/ecommerce_db",
)
SCHEMA = "raw"

# Retry settings (simple, minimal)
RETRY_ATTEMPTS = 2
RETRY_SLEEP_SEC = 2


def get_engine():
    return create_engine(DB_URI)


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
    Load data into PostgreSQL.
    Dùng TRUNCATE + append thay vì replace để:
    - Bảo vệ cấu hình bảng (Indexes, Constraints).
    - Đảm bảo idempotency (chạy lại nhiều lần không nhân bản dữ liệu).
    """
    df = read_dataframe(file_path)
    engine = get_engine()

    with engine.begin() as conn:
        # Clear data but keep table structure
        conn.execute(text(f'TRUNCATE TABLE {SCHEMA}."{table_name}"'))

        # Append fresh data into the existing table
        df.to_sql(
            name=table_name,
            con=conn,
            schema=SCHEMA,
            if_exists="append",
            index=False,
            chunksize=5000,
            method="multi",
        )
    return len(df)

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

from __future__ import annotations

import os
import time
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

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

DATA_DIR = Path("data")
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


def load_table(file_path: Path, table_name: str) -> int:
    df = pd.read_csv(file_path)
    engine = get_engine()
    df.to_sql(
        name=table_name,
        con=engine,
        schema=SCHEMA,
        if_exists="replace",
        index=False,
    )
    return len(df)


def run_ingestion() -> None:
    for filename, table_name in DATASET_CONFIG.items():
        file_path = DATA_DIR / filename

        if not file_path.exists():
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

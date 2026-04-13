"""
=============================================================================
FILE: ingestion/load_streaming.py
=============================================================================
Purpose : Đọc file JSON Lines từ S3 raw/streaming/ và load vào
          raw.streaming_orders trong PostgreSQL (RDS).

Chạy bởi Airflow DAG mỗi 5 phút (schedule="*/5 * * * *").

Design decisions (Senior DE):
  - Checkpoint-based: lưu key đã xử lý → idempotent, không load lại.
  - GZIP-aware: Kinesis Firehose nén file theo mặc định.
  - Consistent với load_csv.py: env-var DB config, module-level logger.
  - Append-only (not TRUNCATE): streaming table tích lũy data theo thời gian.
  - Paginated S3 listing: xử lý >1000 objects không bị mất.
=============================================================================
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pandas as pd
from sqlalchemy import create_engine, text

# Module-level logger — nhất quán với load_csv.py
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
S3_BUCKET  = os.environ.get("S3_BUCKET",  "olist-de-tanphat-2026")
S3_PREFIX  = "raw/streaming/"
SCHEMA     = "raw"
TABLE      = "streaming_orders"

# Checkpoint: lưu key S3 đã xử lý thành công.
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
CHECKPOINT_FILE = Path(os.environ.get(
    "STREAMING_CHECKPOINT_FILE",
    PROJECT_ROOT / "tmp" / "streaming_checkpoint.txt",
))

RETRY_ATTEMPTS  = 2
RETRY_SLEEP_SEC = 3


# ── DB URI — nhất quán với load_csv.py ───────────────────────────────────────
def _build_db_uri() -> str:
    """Build PostgreSQL URI từ env vars — KHÔNG hardcode credential."""
    pg_user     = os.getenv("POSTGRES_USER",     "de_user")
    pg_password = os.getenv("POSTGRES_PASSWORD", "de_password")
    pg_host     = os.getenv("POSTGRES_HOST",     "postgres")   # Docker service name
    pg_port     = os.getenv("POSTGRES_PORT",     "5432")
    pg_db       = os.getenv("POSTGRES_DB",       "ecommerce_db")
    uri = f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    logger.info(
        "[load_streaming] DB → host=%s port=%s db=%s user=%s",
        pg_host, pg_port, pg_db, pg_user,
    )
    return uri


# Evaluated once at import time
DB_URI: str = os.getenv("DB_URI") or _build_db_uri()


# ── Checkpoint helpers ────────────────────────────────────────────────────────
def get_processed_files() -> set[str]:
    """Đọc danh sách S3 key đã xử lý từ checkpoint file."""
    if CHECKPOINT_FILE.exists():
        content = CHECKPOINT_FILE.read_text(encoding="utf-8").strip()
        return set(content.splitlines()) if content else set()
    return set()


def save_checkpoint(processed: set[str]) -> None:
    """Ghi checkpoint atomically (write → rename sẽ tốt hơn nhưng /tmp là ok)."""
    CHECKPOINT_FILE.write_text(
        "\n".join(sorted(processed)),
        encoding="utf-8",
    )
    logger.debug("[load_streaming] Checkpoint saved → %d entries", len(processed))


# ── S3 helpers ────────────────────────────────────────────────────────────────
def list_s3_files(prefix: str) -> list[str]:
    """
    Liệt kê tất cả .json và .gz files trong S3 prefix.
    Paginate qua ContinuationToken để xử lý >1000 objects.
    """
    s3     = boto3.client("s3")
    keys: list[str] = []
    kwargs: dict    = {"Bucket": S3_BUCKET, "Prefix": prefix}

    while True:
        response = s3.list_objects_v2(**kwargs)
        for obj in response.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".gz") or key.endswith(".json"):
                keys.append(key)

        if response.get("IsTruncated"):
            kwargs["ContinuationToken"] = response["NextContinuationToken"]
        else:
            break

    logger.info("[load_streaming] Found %d files under s3://%s/%s", len(keys), S3_BUCKET, prefix)
    return keys


def read_s3_jsonl(key: str) -> list[dict]:
    """
    Đọc file JSON Lines từ S3.
    Tự động giải nén GZIP (Kinesis Firehose mặc định nén).
    Bỏ qua line không hợp lệ thay vì crash toàn bộ file.
    """
    s3  = boto3.client("s3")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    raw = obj["Body"].read()

    if key.endswith(".gz"):
        raw = gzip.decompress(raw)

    records: list[dict] = []
    for line in raw.decode("utf-8").strip().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            logger.warning("[load_streaming] Skipping invalid JSON in %s: %.80s", key, line)

    logger.debug("[load_streaming] Read %d records from %s", len(records), key)
    return records


# ── PostgreSQL loader ─────────────────────────────────────────────────────────
def load_to_postgres(records: list[dict]) -> int:
    """
    Load records vào raw.streaming_orders.
    Dùng if_exists='append': streaming table KHÔNG bị truncate, data tích lũy.
    Bảng phải được tạo trước qua init.sql hoặc CREATE TABLE IF NOT EXISTS.
    """
    if not records:
        return 0

    df = pd.DataFrame(records)

    # Chuẩn hoá timestamp — tránh type mismatch khi records thiếu field
    df["loaded_at"] = datetime.now(timezone.utc)

    engine = create_engine(DB_URI)
    with engine.begin() as conn:
        # Tạo bảng nếu chưa tồn tại (safety net — ideal: schema từ init.sql)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS raw.streaming_orders (
                order_id         VARCHAR(50),
                customer_id      VARCHAR(50),
                product_id       VARCHAR(50),
                seller_id        VARCHAR(50),
                price            NUMERIC(10,2),
                payment_type     VARCHAR(30),
                order_status     VARCHAR(20),
                event_timestamp  TIMESTAMP,
                loaded_at        TIMESTAMPTZ DEFAULT NOW()
            )
        """))

        df.to_sql(
            name=TABLE,
            con=conn,
            schema=SCHEMA,
            if_exists="append",
            index=False,
            chunksize=1000,
            method="multi",
        )

    logger.info("[load_streaming] Loaded %d rows → %s.%s", len(df), SCHEMA, TABLE)
    return len(df)


# ── Main ──────────────────────────────────────────────────────────────────────
def run_streaming_ingestion() -> None:
    """
    Entry-point được Airflow gọi.
    Chỉ xử lý file mới (chưa có trong checkpoint). Idempotent.
    """
    logger.info(
        "[%s] Starting streaming ingestion (bucket=%s prefix=%s)",
        datetime.utcnow().isoformat(), S3_BUCKET, S3_PREFIX,
    )

    all_files       = list_s3_files(S3_PREFIX)
    processed_files = get_processed_files()
    new_files       = [f for f in all_files if f not in processed_files]

    logger.info(
        "[load_streaming] Total S3 files: %d | Already processed: %d | New: %d",
        len(all_files), len(processed_files), len(new_files),
    )

    if not new_files:
        logger.info("[load_streaming] No new files — skipping.")
        return

    total_rows = 0
    for key in new_files:
        logger.info("[load_streaming] Processing → %s", key)
        attempt = 0
        while True:
            try:
                records   = read_s3_jsonl(key)
                rows      = load_to_postgres(records)
                total_rows += rows
                logger.info("[load_streaming] ✅ %s → %d rows", key, rows)
                processed_files.add(key)
                break
            except Exception as exc:  # noqa: BLE001
                attempt += 1
                logger.warning(
                    "[load_streaming] ⚠️ %s failed (attempt %d/%d): %s",
                    key, attempt, RETRY_ATTEMPTS, exc,
                )
                if attempt >= RETRY_ATTEMPTS:
                    logger.error("[load_streaming] ❌ Giving up on %s", key)
                    break
                time.sleep(RETRY_SLEEP_SEC)

    # Chỉ save checkpoint sau khi tất cả mới-file đã được xử lý
    save_checkpoint(processed_files)
    logger.info("[load_streaming] DONE — total rows loaded this run: %d", total_rows)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    run_streaming_ingestion()

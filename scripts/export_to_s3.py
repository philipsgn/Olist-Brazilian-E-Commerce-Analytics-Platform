import os
import io
import logging
import tempfile
from pathlib import Path
import pandas as pd
import boto3
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

S3_BUCKET = os.getenv("S3_BUCKET", "olist-de-tanphat-2026")
S3_PREFIX = "processed/"
TABLES_TO_EXPORT = ["mart_revenue_daily", "fact_payments", "fact_order_items"]

def _build_db_uri() -> str:
    pg_user     = os.getenv("POSTGRES_USER",     "de_user")
    pg_password = os.getenv("POSTGRES_PASSWORD", "de_password")
    pg_host     = os.getenv("POSTGRES_HOST",     "postgres") # Default "postgres" cho Docker Compose / Cloud
    pg_port     = os.getenv("POSTGRES_PORT",     "5432")
    pg_db       = os.getenv("POSTGRES_DB",       "ecommerce_db")
    return f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"

def export_table_to_s3(table_name: str, schema: str = "analytics"):
    try:
        engine = create_engine(_build_db_uri())
        logger.info(f"Đọc dữ liệu từ bảng {schema}.{table_name}...")
        
        # Read from postgres
        df = pd.read_sql(f"SELECT * FROM {schema}.{table_name}", con=engine)
        
        if df.empty:
            logger.warning(f"Bảng {table_name} không có dữ liệu, bỏ qua.")
            return

        # Tính năng chuẩn Production: Partition Projection (chia year/month/day)
        partition_cols = []
        if 'year_number' in df.columns:
            # Chuẩn hóa tên cột để khớp với AWS Athena Partition Projection
            df = df.rename(columns={'year_number': 'year', 'month_number': 'month', 'day_number': 'day'})
            partition_cols = ['year', 'month', 'day']

        logger.info(f"Đang nén Snappy và chia Partition (nếu có)...")
        with tempfile.TemporaryDirectory() as tmp_dir:
            if partition_cols:
                df.to_parquet(tmp_dir, engine='pyarrow', compression='snappy', partition_cols=partition_cols)
            else:
                df.to_parquet(os.path.join(tmp_dir, f"{table_name}.parquet"), engine='pyarrow', compression='snappy')
            
            # Upload toàn bộ thư mục (bao gồm các thư mục con phân mảnh) lên S3
            s3_client = boto3.client("s3")
            for root, dirs, files in os.walk(tmp_dir):
                for file in files:
                    local_path = os.path.join(root, file)
                    rel_path = os.path.relpath(local_path, tmp_dir).replace('\\', '/')
                    s3_key = f"{S3_PREFIX}{table_name}/{rel_path}"
                    
                    logger.info(f"  -> Uploading s3://{S3_BUCKET}/{s3_key}...")
                    s3_client.upload_file(local_path, S3_BUCKET, s3_key)
                    
        logger.info(f"✅ Đã export thành công bảng {table_name} lên S3!")
        
    except Exception as e:
        logger.error(f"❌ Lỗi export bảng {table_name}: {e}")

def main():
    logger.info("🚀 Bắt đầu quá trình Export dữ liệu Processed lên S3...")
    for table in TABLES_TO_EXPORT:
        export_table_to_s3(table)

if __name__ == "__main__":
    main()

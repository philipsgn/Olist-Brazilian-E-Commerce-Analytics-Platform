import os
import time
import logging
import boto3
import pandas as pd
from prophet import Prophet
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configurations
S3_BUCKET = os.getenv("S3_BUCKET", "olist-de-tanphat-2026")
ATHENA_DATABASE = "default"
ATHENA_TABLE = "view_order_analytics_gold"
ATHENA_OUTPUT = f"s3://{S3_BUCKET}/athena-results/"
FORECAST_OUTPUT = f"s3://{S3_BUCKET}/gold/sales_forecast/"

def run_athena_query(query: str):
    """Chạy query trên Athena và đợi kết quả."""
    client = boto3.client('athena')
    
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
    )
    query_execution_id = response['QueryExecutionId']
    
    # Wait for execution
    while True:
        status = client.get_query_execution(QueryExecutionId=query_execution_id)['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(2)
    
    if status != 'SUCCEEDED':
        raise Exception(f"Athena query failed with status: {status}")
    
    # Lấy đường dẫn file kết quả trên S3
    result_file = f"{ATHENA_OUTPUT}{query_execution_id}.csv"
    return result_file

def main():
    logger.info("🚀 Bắt đầu quy trình Sales Forecasting (AI)...")
    
    # 1. Trích xuất dữ liệu từ Athena (Gold Layer)
    # Lấy doanh thu tổng hợp theo ngày
    query = f"""
        SELECT 
            date_trunc('day', created_at) as ds,
            SUM(total_amount) as y
        FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
        WHERE created_at IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    """
    
    try:
        csv_path = run_athena_query(query)
        logger.info(f"Đã lấy dữ liệu từ Athena: {csv_path}")
        
        # Đọc dữ liệu vào Pandas
        df = pd.read_csv(csv_path)
        df['ds'] = pd.to_datetime(df['ds'])
        
        if len(df) < 2:
            logger.warning("Không đủ dữ liệu để dự báo (Cần ít nhất 2 ngày).")
            return

        # 2. Huấn luyện mô hình Prophet
        logger.info("Đang huấn luyện mô hình Prophet...")
        model = Prophet(
            yearly_seasonality=True, 
            weekly_seasonality=True, 
            daily_seasonality=False,
            changepoint_prior_scale=0.05
        )
        model.fit(df)

        # 3. Dự báo cho 7 ngày tiếp theo
        logger.info("Đang tạo dự báo cho 7 ngày tới...")
        future = model.make_future_dataframe(periods=7)
        forecast = model.predict(future)

        # Chỉ lấy các cột cần thiết và dòng tương lai
        # yhat: giá trị dự báo, yhat_lower/upper: khoảng tin cậy
        result_df = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(7)
        result_df['prediction_date'] = datetime.now().strftime('%Y-%m-%d')
        
        # 4. Lưu kết quả lên S3 (Gold Layer)
        local_file = "sales_forecast.parquet"
        result_df.to_parquet(local_file, compression='snappy')
        
        s3_client = boto3.client('s3')
        s3_key = "gold/sales_forecast/sales_forecast.parquet"
        s3_client.upload_file(local_file, S3_BUCKET, s3_key)
        
        logger.info(f"✅ Đã lưu dự báo lên s3://{S3_BUCKET}/{s3_key}")
        
        # Dọn dẹp local
        if os.path.exists(local_file):
            os.remove(local_file)

    except Exception as e:
        logger.error(f"❌ Lỗi trong quá trình AI Forecasting: {e}")
        raise

if __name__ == "__main__":
    main()

import os
import time
import logging
import boto3
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from datetime import datetime, timedelta

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configurations
S3_BUCKET = os.getenv("S3_BUCKET", "olist-de-tanphat-2026")
ATHENA_DATABASE = "default"
ATHENA_TABLE = "view_order_analytics_gold"
ATHENA_OUTPUT = f"s3://{S3_BUCKET}/athena-results/"

def run_athena_query(query: str):
    client = boto3.client('athena')
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
    )
    query_execution_id = response['QueryExecutionId']
    while True:
        status = client.get_query_execution(QueryExecutionId=query_execution_id)['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']: break
        time.sleep(2)
    if status != 'SUCCEEDED': raise Exception(f"Athena query failed: {status}")
    return f"{ATHENA_OUTPUT}{query_execution_id}.csv"

def main():
    logger.info("🚀 Bắt đầu AI Sales Forecasting (Lightweight Edition)...")
    
    query = f"""
        SELECT 
            date_trunc('day', created_at) as ds,
            SUM(total_amount) as y
        FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
        WHERE created_at IS NOT NULL
        GROUP BY 1 ORDER BY 1
    """
    
    try:
        csv_path = run_athena_query(query)
        df = pd.read_csv(csv_path)
        df['ds'] = pd.to_datetime(df['ds'])
        
        if len(df) < 5:
            logger.warning("Không đủ dữ liệu (cần ít nhất 5 ngày).")
            return

        # 1. Feature Engineering siêu nhẹ: Chuyển ngày thành số thứ tự
        df['day_index'] = (df['ds'] - df['ds'].min()).dt.days
        X = df[['day_index']].values
        y = df['y'].values

        # 2. Train mô hình Linear Regression
        model = LinearRegression()
        model.fit(X, y)

        # 3. Dự báo cho 7 ngày tiếp theo
        last_day = df['day_index'].max()
        future_indices = np.array([last_day + i for i in range(1, 8)]).reshape(-1, 1)
        predictions = model.predict(future_indices)

        # 4. Tạo DataFrame kết quả
        last_date = df['ds'].max()
        forecast_dates = [last_date + timedelta(days=i) for i in range(1, 8)]
        
        result_df = pd.DataFrame({
            'ds': forecast_dates,
            'yhat': predictions,
            'yhat_lower': predictions * 0.9, # Giả lập khoảng tin cậy
            'yhat_upper': predictions * 1.1,
            'prediction_date': [datetime.now().strftime('%Y-%m-%d')] * 7
        })

        # 5. Lưu kết quả lên S3
        local_file = "sales_forecast.parquet"
        result_df.to_parquet(local_file, compression='snappy')
        
        s3_client = boto3.client('s3')
        s3_client.upload_file(local_file, S3_BUCKET, "gold/sales_forecast/sales_forecast.parquet")
        
        logger.info(f"✅ Đã lưu dự báo (Lightweight) lên S3!")
        if os.path.exists(local_file): os.remove(local_file)

    except Exception as e:
        logger.error(f"❌ Lỗi AI Forecasting: {e}")
        raise

if __name__ == "__main__":
    main()

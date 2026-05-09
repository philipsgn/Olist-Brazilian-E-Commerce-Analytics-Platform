import os
import time
import logging
import boto3
import pandas as pd
import numpy as np
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error
from datetime import datetime, timedelta

# --- Senior Level Logging Setup ---
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s: %(message)s')
logger = logging.getLogger("SalesForecaster")
logger.info("Forecasting Engine Senior v2 Initialized")

# Environment & Constants
S3_BUCKET = os.getenv("S3_BUCKET", "olist-de-tanphat-2026")
ATHENA_DATABASE = "default"
ATHENA_TABLE = "view_order_analytics_gold"
ATHENA_OUTPUT = f"s3://{S3_BUCKET}/athena-results/"

class SalesForecaster:
    """Senior-grade forecasting engine optimized for stability and memory efficiency."""
    
    def __init__(self):
        self.athena = boto3.client('athena')
        self.s3 = boto3.client('s3')

    def fetch_data(self):
        query = f"""
            SELECT 
                date_trunc('day', created_at) as ds,
                SUM(total_amount) as y
            FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
            WHERE created_at IS NOT NULL
            GROUP BY 1 ORDER BY 1
        """
        logger.info(f"Fetching data from Athena: {ATHENA_DATABASE}.{ATHENA_TABLE}")
        
        response = self.athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': ATHENA_DATABASE},
            ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
        )
        query_id = response['QueryExecutionId']
        
        # Robust polling mechanism
        while True:
            status = self.athena.get_query_execution(QueryExecutionId=query_id)['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']: break
            time.sleep(2)
            
        if status != 'SUCCEEDED':
            raise RuntimeError(f"Athena query failed with status: {status}")
            
        csv_path = f"{ATHENA_OUTPUT}{query_id}.csv"
        return pd.read_csv(csv_path)

    def prepare_features(self, df):
        """Advanced Feature Engineering: Seasonality & Trend"""
        df['ds'] = pd.to_datetime(df['ds'])
        df = df.sort_values('ds')
        
        # Handle missing dates (Fill with 0 or interpolation)
        df = df.set_index('ds').resample('D').sum().reset_index()
        
        # Features: Day of week (Seasonality) + Day index (Trend)
        df['day_index'] = (df['ds'] - df['ds'].min()).dt.days
        df['day_of_week'] = df['ds'].dt.dayofweek
        
        # One-hot encoding for day of week (Senior practice: handling weekly cycles)
        for i in range(7):
            df[f'is_day_{i}'] = (df['day_of_week'] == i).astype(int)
            
        return df

    def train_and_predict(self, df):
        feature_cols = ['day_index'] + [f'is_day_{i}' for i in range(7)]
        X = df[feature_cols].values
        y = df['y'].values
        
        # Use Ridge Regression (Senior choice: handles multi-collinearity better than simple Linear)
        model = Ridge(alpha=1.0)
        model.fit(X, y)
        
        # Metric tracking
        y_pred_train = model.predict(X)
        mae = mean_absolute_error(y, y_pred_train)
        logger.info(f"Model Training Complete. MAE: {mae:.2f}")
        
        # Forecast 7 days
        last_date = df['ds'].max()
        future_dates = [last_date + timedelta(days=i) for i in range(1, 8)]
        
        future_df = pd.DataFrame({'ds': future_dates})
        future_df['day_index'] = (future_df['ds'] - df['ds'].min()).dt.days
        future_df['day_of_week'] = future_df['ds'].dt.dayofweek
        for i in range(7):
            future_df[f'is_day_{i}'] = (future_df['day_of_week'] == i).astype(int)
            
        predictions = model.predict(future_df[feature_cols].values)
        
        # Add Confidence Intervals (Simulated for visualization)
        future_df['yhat'] = np.maximum(0, predictions) # Ensure no negative sales
        future_df['yhat_lower'] = future_df['yhat'] * 0.85
        future_df['yhat_upper'] = future_df['yhat'] * 1.15
        future_df['prediction_date'] = datetime.now().strftime('%Y-%m-%d')
        
        return future_df[['ds', 'yhat', 'yhat_lower', 'yhat_upper', 'prediction_date']]

    def execute(self):
        try:
            raw_df = self.fetch_data()
            if len(raw_df) < 7:
                logger.warning("Insufficient data for reliable forecasting (Need at least 7 days).")
                return
                
            processed_df = self.prepare_features(raw_df)
            forecast_df = self.train_and_predict(processed_df)
            
            # Export to S3 as Parquet (Standard for Gold Layer)
            output_file = "sales_forecast.parquet"
            forecast_df.to_parquet(output_file, index=False, compression='snappy')
            
            self.s3.upload_file(output_file, S3_BUCKET, "gold/sales_forecast/sales_forecast.parquet")
            logger.info("✅ Forecast successfully exported to S3 Gold Layer.")
            os.remove(output_file)
            
        except Exception as e:
            logger.error(f"Fatal error in forecasting pipeline: {str(e)}")
            raise

def main():
    """Main entry point for Airflow PythonOperator"""
    logger.info("🚀 Bắt đầu AI Sales Forecasting (Senior v2 - Optimized)...")
    forecaster = SalesForecaster()
    forecaster.execute()

if __name__ == "__main__":
    main()

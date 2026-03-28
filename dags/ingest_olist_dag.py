# dags/ingest_olist_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def ingest_orders():
    import pandas as pd
    from sqlalchemy import create_engine
    df = pd.read_csv('/data/olist_orders_dataset.csv')
    engine = create_engine('postgresql://...')
    df.to_sql('raw_orders', engine, schema='raw', if_exists='replace')

with DAG('ingest_olist', start_date=datetime(2024,1,1), schedule='@daily') as dag:
    task = PythonOperator(task_id='ingest_orders', python_callable=ingest_orders)
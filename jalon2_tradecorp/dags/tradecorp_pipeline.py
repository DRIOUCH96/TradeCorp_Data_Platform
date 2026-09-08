from airflow import DAG
from datetime import datetime, timedelta
default_args = {
    'owner': 'airflow', 
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
with DAG(
    dag_id="tradecorp_etl_pipeline",
    default_args=default_args,
    description="Pipeline de traitement des données TradeCorp",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["tradecorp", "etl","spark"],
) as dag:
    pass  # Les tâches seront définies ici
    
    
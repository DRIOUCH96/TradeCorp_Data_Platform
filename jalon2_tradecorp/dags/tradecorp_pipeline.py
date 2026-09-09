from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from docker.types import Mount
import os
from airflow.sensors.filesystem import FileSensor
default_args = {
    'owner': 'tradecorp', 
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}
PROJECT_PATH = (
    "//c/Users/driou/Downloads/"
    "Formation_Data_Engineer_BeB/"
    "SQL_POSTGRESQL_/"
    "TradeCorp_Data_Platform/"
    "jalon2_tradecorp"
)
MOUNTS = [
    Mount(
        source=f"{PROJECT_PATH}/src",
        target="/home/jovyan/src",
        type="bind",
        read_only=True,
    ),
    Mount(
        source=f"{PROJECT_PATH}/data",
        target="/home/jovyan/data",
        type="bind",
    ),
    Mount(
        source=f"{PROJECT_PATH}/.env",
        target="/home/jovyan/.env",
        type="bind",
        read_only=True,
    ),
]
TASK_ENVIRONMENT = {
    "AZURE_RAW_CONTAINER": os.getenv(
        "AZURE_RAW_CONTAINER",
        "raw",
    ),
    "AZURE_RAW_REFERENCE_PATH": os.getenv(
        "AZURE_RAW_REFERENCE_PATH",
        "reference",
    ),
    "AZURE_CLEAN_CONTAINER": os.getenv(
        "AZURE_CLEAN_CONTAINER",
        "clean",
    ),
    "LOCAL_TMP_DIR": "/home/jovyan/data/tmp",
    "CLEAN_OUTPUT_PATH": os.getenv(
        "CLEAN_OUTPUT_PATH",
        "orders_enriched.parquet",
    ),
    "STAGING_PARQUET_PATH": os.getenv(
        "STAGING_PARQUET_PATH"),
    "COUNTRY_CURRENCY_FILENAME": os.getenv(
        "COUNTRY_CURRENCY_FILENAME",
        "country_currency.csv",
    ),
    "CLEAN_OUTPUT_PATH": os.getenv(
        "CLEAN_OUTPUT_PATH",
        "orders_enriched",
    ),
}


PRIVATE_ENVIRONMENT = {
    "AZURE_STORAGE_ACCOUNT_NAME": os.getenv(
        "AZURE_STORAGE_ACCOUNT_NAME",
        "",
    ),
    "AZURE_STORAGE_ACCOUNT_KEY": os.getenv(
        "AZURE_STORAGE_ACCOUNT_KEY",
        "",
    ),
}
def create_task(task_id: str, command: str) -> DockerOperator:
    """Crée une tâche DockerOperator pour Airflow."""

    return DockerOperator(
        task_id=task_id,
        image="tradecorp-spark:latest",
        command=command,
        docker_url="unix://var/run/docker.sock",
        network_mode="tradecorp_default",
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=MOUNTS,
        environment=TASK_ENVIRONMENT,
        private_environment=PRIVATE_ENVIRONMENT,
    )
with DAG(
    dag_id="tradecorp_etl_pipeline",
    default_args=default_args,
    description="Pipeline de traitement des données TradeCorp",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["tradecorp", "etl","spark"],
) as dag:
    wait_for_trigger_file = FileSensor(
        task_id="wait_for_trigger_file",
        filepath="/opt/airflow/data/trigger/go.txt",
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=3600,
        mode="poke",
    )
    t0 = create_task(
        task_id="fetch_exchange_rates",
        command="python /home/jovyan/src/fetch_exchange_rates.py"
    )
    t1 = create_task(
        task_id="reader",
        command="spark-submit /home/jovyan/src/reader.py"
    )
    t2 = create_task(
        task_id="transformer",
        command="spark-submit /home/jovyan/src/transformer.py"
    )
    t3 = create_task(
        task_id="writer",
        command="spark-submit /home/jovyan/src/writer.py"
    )
    wait_for_trigger_file >> t0 >> t1 >> t2 >> t3  # Définition des dépendances entre les tâches
    
    
    
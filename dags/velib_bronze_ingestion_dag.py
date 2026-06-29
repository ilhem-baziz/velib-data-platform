from datetime import datetime, timedelta
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Permet à Airflow de trouver les fichiers dans /opt/airflow/src
sys.path.append("/opt/airflow/src")

from extract.fetch_stations import fetch_stations
from extract.fetch_availability import fetch_availability


default_args = {
    "owner": "velibdata",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="velib_bronze_ingestion",
    description="Ingestion Bronze des APIs Vélib vers MinIO",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["velib", "bronze", "ingestion"],
) as dag:

    task_fetch_stations = PythonOperator(
        task_id="recuperer_informations_stations",
        python_callable=fetch_stations,
    )

    task_fetch_availability = PythonOperator(
        task_id="recuperer_disponibilites_stations",
        python_callable=fetch_availability,
    )

    task_trigger_silver = TriggerDagRunOperator(
        task_id="declencher_transformation_silver",
        trigger_dag_id="velib_silver_transformation",
        wait_for_completion=False,
    )

    task_fetch_stations >> task_fetch_availability >> task_trigger_silver
from datetime import datetime, timedelta
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

sys.path.append("/opt/airflow/src")

from transform.bronze_to_silver import run_bronze_to_silver

default_args = {
    "owner": "velibdata",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="velib_silver_transformation",
    description="Transformation Bronze vers Silver",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["velib", "silver", "transformation", "qualite"],
) as dag:

    task_bronze_to_silver = PythonOperator(
        task_id="transformer_bronze_vers_silver",
        python_callable=run_bronze_to_silver,
    )

    task_trigger_gold = TriggerDagRunOperator(
        task_id="declencher_transformation_gold",
        trigger_dag_id="velib_gold_transformation",
        wait_for_completion=False,
    )

    task_bronze_to_silver >> task_trigger_gold
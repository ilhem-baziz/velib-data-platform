from datetime import datetime, timedelta
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow/src")

from transform.silver_to_gold import run_silver_to_gold


default_args = {
    "owner": "velibdata",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="velib_gold_transformation",
    description="Construction des indicateurs Gold depuis Silver",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["velib", "gold", "transformation"],
) as dag:

    task_silver_to_gold = PythonOperator(
        task_id="construire_gold",
        python_callable=run_silver_to_gold,
    )
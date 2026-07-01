from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "velibdata",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


SPARK_MONITORING_CMD = """
spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=airflow-scheduler \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.hadoop.fs.defaultFS=hdfs://hdfs-namenode:9000 \
  --conf spark.driverEnv.PYTHONPATH=/opt/airflow/src \
  --conf spark.executorEnv.PYTHONPATH=/opt/app/src \
  /opt/airflow/src/monitoring/monitoring_checks.py
"""


with DAG(
    dag_id="velib_monitoring",
    description="Supervision HDFS, Gold PostgreSQL et qualite des donnees",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["velib", "monitoring", "hdfs", "quality"],
) as dag:

    task_monitoring_checks = BashOperator(
        task_id="run_monitoring_checks",
        bash_command=SPARK_MONITORING_CMD,
    )
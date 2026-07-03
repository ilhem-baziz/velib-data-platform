from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


default_args = {
    "owner": "velibdata",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


SPARK_BRONZE_TO_SILVER_CMD = """
spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --total-executor-cores 6 \
  --executor-cores 2 \
  --executor-memory 1g \
  --conf spark.driver.host=airflow-scheduler \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.hadoop.fs.defaultFS=hdfs://hdfs-namenode:9000 \
  --conf spark.hadoop.dfs.replication=2 \
  --conf spark.driverEnv.PYTHONPATH=/opt/airflow/src \
  --conf spark.executorEnv.PYTHONPATH=/opt/app/src \
  /opt/airflow/src/spark/bronze_to_silver_spark.py
"""


with DAG(
    dag_id="velib_silver_transformation",
    description="Transformation Bronze vers Silver avec Spark et stockage HDFS",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["velib", "silver", "spark", "hdfs"],
) as dag:

    task_bronze_to_silver_spark = BashOperator(
        task_id="spark_bronze_vers_silver_hdfs",
        bash_command=SPARK_BRONZE_TO_SILVER_CMD,
        do_xcom_push=False,
    )

    task_trigger_gold = TriggerDagRunOperator(
        task_id="declencher_transformation_gold",
        trigger_dag_id="velib_gold_transformation",
        wait_for_completion=False,
    )

    task_bronze_to_silver_spark >> task_trigger_gold

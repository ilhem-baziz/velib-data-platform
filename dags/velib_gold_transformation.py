from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "velibdata",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


SPARK_SILVER_TO_GOLD_CMD = """
spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=airflow-scheduler \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.hadoop.fs.defaultFS=hdfs://hdfs-namenode:9000 \
  --conf spark.hadoop.dfs.replication=2 \
  --conf spark.driverEnv.PYTHONPATH=/opt/airflow/src \
  --conf spark.executorEnv.PYTHONPATH=/opt/app/src \
  /opt/airflow/src/spark/silver_to_gold_spark.py
"""


with DAG(
    dag_id="velib_gold_transformation",
    description="Transformation Silver HDFS vers Gold HDFS avec Spark",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["velib", "gold", "spark", "hdfs"],
) as dag:

    task_silver_to_gold = BashOperator(
        task_id="spark_silver_vers_gold_hdfs",
        bash_command=SPARK_SILVER_TO_GOLD_CMD,
    )
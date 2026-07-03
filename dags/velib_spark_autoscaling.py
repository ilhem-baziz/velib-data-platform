from datetime import datetime, timedelta
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


PROMETHEUS_URL = "http://prometheus:9090/api/v1/query"
DOCKER_API_URL = "http://docker-socket-proxy:2375"
SPARK_MASTER_JSON_URL = "http://spark-master:8080/json/"

WORKER3_CONTAINER = "spark-worker-3"

SCALE_UP_CPU_THRESHOLD = 0.005
SCALE_DOWN_CPU_THRESHOLD = 0.001
MIN_MINUTES_BEFORE_SCALE_DOWN = 5
MIN_MINUTES_BEFORE_SCALE_UP = 2


def build_cpu_query(worker3_running: bool) -> str:
    workers = "spark-worker-1|spark-worker-2"
    if worker3_running:
        workers += "|spark-worker-3"
    return (
        f'avg(rate(container_cpu_usage_seconds_total'
        f'{{name=~"{workers}"}}[1m]))'
    )


def get_prometheus_value(query: str) -> float:
    response = requests.get(PROMETHEUS_URL, params={"query": query}, timeout=10)
    response.raise_for_status()

    data = response.json()
    result = data.get("data", {}).get("result", [])

    if not result:
        return 0.0

    return float(result[0]["value"][1])


def is_worker3_running() -> bool:
    try:
        response = requests.get(
            f"{DOCKER_API_URL}/containers/{WORKER3_CONTAINER}/json",
            timeout=10,
        )
    except requests.exceptions.RequestException as e:
        raise Exception(f"Docker Socket Proxy injoignable : {e}")

    if response.status_code == 404:
        raise Exception(f"Conteneur {WORKER3_CONTAINER} introuvable — vérifier docker compose create spark-worker-3")

    if response.status_code != 200:
        raise Exception(f"Erreur inattendue Docker API : {response.status_code} {response.text}")

    data = response.json()
    return bool(data.get("State", {}).get("Running", False))


def start_worker3():
    response = requests.post(
        f"{DOCKER_API_URL}/containers/{WORKER3_CONTAINER}/start",
        timeout=10,
    )

    if response.status_code not in (204, 304):
        raise Exception(f"Erreur démarrage worker3 : {response.status_code} {response.text}")


def stop_worker3():
    response = requests.post(
        f"{DOCKER_API_URL}/containers/{WORKER3_CONTAINER}/stop",
        params={"t": 30},
        timeout=40,
    )

    if response.status_code not in (204, 304):
        raise Exception(f"Erreur arrêt worker3 : {response.status_code} {response.text}")

    Variable.set("spark_worker3_stopped_at", datetime.utcnow().isoformat())


def get_active_spark_applications() -> list:
    try:
        response = requests.get(SPARK_MASTER_JSON_URL, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise Exception(f"Spark Master injoignable : {e}")

    data = response.json()
    return data.get("activeapps", [])


def can_scale_down_safely() -> bool:
    active_apps = get_active_spark_applications()

    if active_apps:
        app_names = [
            app.get("name", "unknown")
            for app in active_apps
        ]
        print(f"[AUTOSCALING] Scale-down refusé : applications Spark actives = {app_names}")
        return False

    print("[AUTOSCALING] Aucune application Spark active : scale-down autorisé")
    return True


def autoscale_spark():
    worker3_running = is_worker3_running()
    cpu_value = get_prometheus_value(build_cpu_query(worker3_running))

    print(f"[AUTOSCALING] CPU avg Spark Workers = {cpu_value}")
    print(f"[AUTOSCALING] spark-worker-3 actif = {worker3_running}")

    if cpu_value >= SCALE_UP_CPU_THRESHOLD and not worker3_running:
        last_stopped = Variable.get("spark_worker3_stopped_at", default_var=None)
        if last_stopped:
            minutes_since_stop = (datetime.utcnow() - datetime.fromisoformat(last_stopped)).total_seconds() / 60
            if minutes_since_stop < MIN_MINUTES_BEFORE_SCALE_UP:
                print(f"[AUTOSCALING] Charge élevée mais anti-flapping actif ({minutes_since_stop:.1f}min < {MIN_MINUTES_BEFORE_SCALE_UP}min)")
                return

        print("[AUTOSCALING] Charge détectée : démarrage automatique de spark-worker-3")
        start_worker3()
        Variable.set("spark_worker3_scaled_at", datetime.utcnow().isoformat())
        print("[AUTOSCALING] spark-worker-3 démarré")

    elif cpu_value <= SCALE_DOWN_CPU_THRESHOLD and worker3_running:
        scaled_at_value = Variable.get("spark_worker3_scaled_at", default_var=None)

        if scaled_at_value:
            scaled_at = datetime.fromisoformat(scaled_at_value)
            minutes_running = (datetime.utcnow() - scaled_at).total_seconds() / 60
        else:
            minutes_running = MIN_MINUTES_BEFORE_SCALE_DOWN + 1

        if minutes_running >= MIN_MINUTES_BEFORE_SCALE_DOWN:
            if not can_scale_down_safely():
                return

            print("[AUTOSCALING] Charge faible et aucun job Spark actif : arrêt automatique de spark-worker-3")
            stop_worker3()
            print("[AUTOSCALING] spark-worker-3 arrêté")
        else:
            print(f"[AUTOSCALING] Charge faible mais délai minimum non atteint ({minutes_running:.1f}min < {MIN_MINUTES_BEFORE_SCALE_DOWN}min)")

    else:
        print("[AUTOSCALING] Aucune action nécessaire")


default_args = {
    "owner": "velibdata",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="velib_spark_autoscaling",
    description="Autoscaling local des Spark Workers à partir des métriques Prometheus",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["velib", "autoscaling", "spark", "prometheus"],
) as dag:

    task_autoscale_spark = PythonOperator(
        task_id="autoscale_spark_worker",
        python_callable=autoscale_spark,
    )

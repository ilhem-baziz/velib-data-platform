import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import psycopg2
from minio import Minio
from pyspark.sql import SparkSession

# ─── Configuration ────────────────────────────────────────────────────────────
# Toutes les valeurs sont surchargées par variables d'environnement Docker.
# Les valeurs par défaut correspondent aux noms de services dans docker-compose.

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "velibdata")
POSTGRES_USER = os.getenv("POSTGRES_USER", "velib_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "velib_password")

HDFS_URI = os.getenv("HDFS_URI", "hdfs://hdfs-namenode:9000")
# Port 9870 = interface HTTP du NameNode (API JMX et WebHDFS)
NAMENODE_HTTP_URL = os.getenv("NAMENODE_HTTP_URL", "http://hdfs-namenode:9870")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "velib")
BRONZE_MAX_DELAY_MINUTES = int(os.getenv("BRONZE_MAX_DELAY_MINUTES", "30"))

# ─── Datasets à surveiller ────────────────────────────────────────────────────
# Chaque entrée : (nom logique, couche, chemin HDFS)
# Le chemin HDFS est relatif à la racine du système de fichiers distribué.
HDFS_DATASETS = [
    ("silver.stations", "Silver", "/data/silver/stations"),
    ("silver.releves", "Silver", "/data/silver/releves"),
    ("gold.disponibilite_courante", "Gold", "/data/gold/disponibilite_courante"),
    ("gold.tendance_horaire", "Gold", "/data/gold/tendance_horaire"),
    ("gold.kpi_reseau_courant", "Gold", "/data/gold/kpi_reseau_courant"),
    ("gold.kpi_reseau_historique", "Gold", "/data/gold/kpi_reseau_historique"),
]

# Tables Gold répliquées dans PostgreSQL pour exposition API / dashboards
POSTGRES_GOLD_TABLES = [
    ("gold.disponibilite_courante", "Gold"),
    ("gold.tendance_horaire", "Gold"),
    ("gold.kpi_reseau_courant", "Gold"),
    ("gold.kpi_reseau_historique", "Gold"),
]


# ─── Connexions ───────────────────────────────────────────────────────────────

def get_postgres_connection():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    # autocommit=True : chaque INSERT est validé immédiatement,
    # pas besoin d'appeler conn.commit() manuellement.
    conn.autocommit = True
    return conn


def create_spark_session():
    # deploy-mode client : le driver tourne dans ce même processus (airflow-scheduler).
    # shuffle.partitions=4 réduit la fragmentation sur un petit cluster de dev.
    return (
        SparkSession.builder
        .appName("velib_monitoring_checks")
        .config("spark.hadoop.fs.defaultFS", HDFS_URI)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# ─── Initialisation du schéma de monitoring ───────────────────────────────────

def ensure_monitoring_tables(conn):
    """Crée les tables de monitoring si elles n'existent pas encore."""
    with conn.cursor() as cursor:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS monitoring;")

        # check_results : une ligne par vérification (OK / FAILED / WARNING)
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS monitoring.check_results (
                id BIGSERIAL PRIMARY KEY,
                check_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                component VARCHAR(100),      -- ex: HDFS, PostgreSQL, Data Quality
                layer_name VARCHAR(50),      -- ex: Silver, Gold, Cluster
                check_name VARCHAR(150),     -- identifiant unique du check
                status VARCHAR(20),          -- OK | FAILED | WARNING
                severity VARCHAR(20),        -- INFO | WARNING | CRITICAL
                metric_value NUMERIC,        -- valeur numérique mesurée
                details TEXT                 -- message lisible humain
            );
            """
        )

        # hdfs_cluster_metrics : métriques globales du cluster HDFS (1 ligne par run)
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS monitoring.hdfs_cluster_metrics (
                id BIGSERIAL PRIMARY KEY,
                check_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                live_nodes INTEGER,
                dead_nodes INTEGER,
                missing_blocks INTEGER,
                under_replicated_blocks INTEGER,
                corrupt_blocks INTEGER,
                capacity_total_gb NUMERIC,
                capacity_used_gb NUMERIC,
                capacity_remaining_gb NUMERIC,
                capacity_used_percent NUMERIC
            );
            """
        )

        # dataset_metrics : statistiques par dataset (lignes, fichiers, fraîcheur)
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS monitoring.dataset_metrics (
                id BIGSERIAL PRIMARY KEY,
                check_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                dataset_name VARCHAR(150),
                layer_name VARCHAR(50),
                storage_type VARCHAR(50),    -- HDFS ou PostgreSQL
                row_count BIGINT,
                file_count INTEGER,          -- nombre de fichiers Parquet (hors _SUCCESS)
                has_success_file BOOLEAN,    -- présence du marqueur _SUCCESS Spark
                last_update TIMESTAMP,       -- dernier mtime de fichier dans le répertoire
                details TEXT
            );
            """
        )


# ─── Fonctions d'insertion ────────────────────────────────────────────────────

def insert_check(conn, component, layer_name, check_name, status, severity, metric_value=None, details=None):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO monitoring.check_results (
                component, layer_name, check_name, status, severity, metric_value, details
            ) VALUES (%s, %s, %s, %s, %s, %s, %s);
            """,
            (component, layer_name, check_name, status, severity, metric_value, details),
        )


def insert_hdfs_cluster_metrics(
    conn,
    live_nodes, dead_nodes,
    missing_blocks, under_replicated_blocks, corrupt_blocks,
    capacity_total_gb, capacity_used_gb, capacity_remaining_gb, capacity_used_percent,
):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO monitoring.hdfs_cluster_metrics (
                live_nodes, dead_nodes, missing_blocks, under_replicated_blocks, corrupt_blocks,
                capacity_total_gb, capacity_used_gb, capacity_remaining_gb, capacity_used_percent
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (
                live_nodes, dead_nodes,
                missing_blocks, under_replicated_blocks, corrupt_blocks,
                capacity_total_gb, capacity_used_gb, capacity_remaining_gb, capacity_used_percent,
            ),
        )


def insert_dataset_metric(
    conn, dataset_name, layer_name, storage_type, row_count,
    file_count=None, has_success_file=None, last_update=None, details=None,
):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO monitoring.dataset_metrics (
                dataset_name, layer_name, storage_type, row_count,
                file_count, has_success_file, last_update, details
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (dataset_name, layer_name, storage_type, row_count,
             file_count, has_success_file, last_update, details),
        )


# ─── API JMX du NameNode ──────────────────────────────────────────────────────

def fetch_jmx_bean(query):
    """
    Interroge l'API JMX HTTP du NameNode et retourne le premier bean trouvé.

    Le NameNode expose ses métriques internes via HTTP sur le port 9870.
    Exemple d'URL : http://hdfs-namenode:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState
    """
    # safe=":=," évite d'encoder les séparateurs JMX qui font partie de la syntaxe de query
    encoded_query = urllib.parse.quote(query, safe=":=,")
    url = f"{NAMENODE_HTTP_URL}/jmx?qry={encoded_query}"
    with urllib.request.urlopen(url, timeout=10) as response:
        payload = json.loads(response.read().decode("utf-8"))
    beans = payload.get("beans", [])
    if not beans:
        raise RuntimeError(f"Aucun bean JMX retourne pour {query}")
    return beans[0]


def bytes_to_gb(value):
    return round(float(value) / (1024 ** 3), 3)


# ─── Check 1 : Santé du cluster HDFS ─────────────────────────────────────────

def check_hdfs_cluster(conn):
    """
    Vérifie l'état global du cluster HDFS via l'API JMX du NameNode.

    FSNamesystemState  → état des blocs et de la capacité disque
    NameNodeInfo       → liste des DataNodes vivants / morts (JSON embarqué)
    """
    fs_state = fetch_jmx_bean("Hadoop:service=NameNode,name=FSNamesystemState")
    fs_info = fetch_jmx_bean("Hadoop:service=NameNode,name=NameNodeInfo")

    # LiveNodes / DeadNodes sont des chaînes JSON imbriquées dans le bean JMX
    live_nodes = len(json.loads(fs_info.get("LiveNodes", "{}")))
    dead_nodes = len(json.loads(fs_info.get("DeadNodes", "{}")))
    missing_blocks = int(fs_state.get("MissingBlocks", 0))
    under_replicated_blocks = int(fs_state.get("UnderReplicatedBlocks", 0))
    corrupt_blocks = int(fs_state.get("CorruptBlocks", 0))

    capacity_total = float(fs_state.get("CapacityTotal", 0))
    capacity_used = float(fs_state.get("CapacityUsed", 0))
    capacity_remaining = float(fs_state.get("CapacityRemaining", 0))
    # Protection division par zéro si le cluster est vide
    capacity_used_percent = round((capacity_used / capacity_total) * 100, 2) if capacity_total > 0 else 0

    insert_hdfs_cluster_metrics(
        conn,
        live_nodes, dead_nodes,
        missing_blocks, under_replicated_blocks, corrupt_blocks,
        bytes_to_gb(capacity_total),
        bytes_to_gb(capacity_used),
        bytes_to_gb(capacity_remaining),
        capacity_used_percent,
    )

    # Seuil : au moins 2 DataNodes pour garantir la réplication par défaut (factor=2)
    insert_check(conn, "HDFS", "Cluster", "DATANODES_LIVE",
                 "OK" if live_nodes >= 2 else "FAILED",
                 "INFO" if live_nodes >= 2 else "CRITICAL",
                 live_nodes, f"{live_nodes} DataNode(s) vivant(s)")

    insert_check(conn, "HDFS", "Cluster", "DATANODES_DEAD",
                 "OK" if dead_nodes == 0 else "FAILED",
                 "INFO" if dead_nodes == 0 else "CRITICAL",
                 dead_nodes, f"{dead_nodes} DataNode(s) mort(s)")

    # MissingBlocks = blocs sans aucune réplique disponible → perte de données potentielle
    insert_check(conn, "HDFS", "Cluster", "MISSING_BLOCKS",
                 "OK" if missing_blocks == 0 else "FAILED",
                 "INFO" if missing_blocks == 0 else "CRITICAL",
                 missing_blocks, f"{missing_blocks} bloc(s) manquant(s)")

    # CorruptBlocks = blocs dont la somme de contrôle est invalide → intégrité des données compromise
    insert_check(conn, "HDFS", "Cluster", "CORRUPT_BLOCKS",
                 "OK" if corrupt_blocks == 0 else "FAILED",
                 "INFO" if corrupt_blocks == 0 else "CRITICAL",
                 corrupt_blocks, f"{corrupt_blocks} bloc(s) corrompu(s)")

    # UnderReplicated = blocs avec moins de répliques que le facteur cible (WARNING, pas CRITICAL)
    insert_check(conn, "HDFS", "Cluster", "UNDER_REPLICATED_BLOCKS",
                 "OK" if under_replicated_blocks == 0 else "WARNING",
                 "INFO" if under_replicated_blocks == 0 else "WARNING",
                 under_replicated_blocks, f"{under_replicated_blocks} bloc(s) sous-replique(s)")

    # Seuil 80% : au-delà le cluster commence à refuser des écritures
    insert_check(conn, "HDFS", "Storage", "CAPACITY_USED_PERCENT",
                 "OK" if capacity_used_percent < 80 else "WARNING",
                 "INFO" if capacity_used_percent < 80 else "WARNING",
                 capacity_used_percent, f"Utilisation HDFS : {capacity_used_percent}%")


# ─── Utilitaires HDFS via API Java (JVM bridge PySpark) ──────────────────────

def get_hdfs_filesystem(spark):
    """
    Retourne l'objet FileSystem Hadoop via le bridge JVM de PySpark.
    Cela permet d'appeler les API Java HDFS directement depuis Python
    sans passer par des commandes shell (hdfs dfs).
    """
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(
        jvm.java.net.URI.create(HDFS_URI),
        conf,
    )
    return fs, jvm


def hdfs_exists(fs, jvm, path):
    return fs.exists(jvm.org.apache.hadoop.fs.Path(path))


def hdfs_count_files(fs, jvm, path):
    """Compte récursivement les fichiers de données (ignore _SUCCESS, .crc, etc.)."""
    hdfs_path = jvm.org.apache.hadoop.fs.Path(path)
    if not fs.exists(hdfs_path):
        return 0
    total = 0
    for status in fs.listStatus(hdfs_path):
        name = status.getPath().getName()
        if status.isDirectory():
            # Descente récursive dans les sous-répertoires (partitions Spark)
            total += hdfs_count_files(fs, jvm, status.getPath().toString())
        elif not name.startswith("_") and not name.startswith("."):
            # Exclut _SUCCESS, _metadata, .crc…
            total += 1
    return total


def hdfs_last_update(fs, jvm, path):
    """Retourne le timestamp de modification le plus récent parmi les fichiers du répertoire."""
    hdfs_path = jvm.org.apache.hadoop.fs.Path(path)
    if not fs.exists(hdfs_path):
        return None
    max_timestamp_ms = 0
    for status in fs.listStatus(hdfs_path):
        max_timestamp_ms = max(max_timestamp_ms, status.getModificationTime())
    if max_timestamp_ms == 0:
        return None
    # getModificationTime() retourne des millisecondes epoch → conversion en datetime Python
    return datetime.fromtimestamp(max_timestamp_ms / 1000)


# ─── Check Bronze : Datasets MinIO ───────────────────────────────────────────

def check_bronze_minio(conn):
    print("Checking Bronze MinIO datasets...")

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    bronze_datasets = {
        "bronze.station_information": "bronze/station_information/",
        "bronze.station_status": "bronze/station_status/",
    }

    now = datetime.now(timezone.utc)

    for dataset_name, prefix in bronze_datasets.items():
        try:
            objects = list(client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True))

            json_objects = [
                obj for obj in objects
                if obj.object_name.endswith(".json")
            ]

            file_count = len(json_objects)

            if file_count == 0:
                insert_check(conn, "MinIO", "Bronze", "BRONZE_FILES_EXIST",
                             "FAILED", "CRITICAL",
                             0, f"Aucun fichier Bronze trouvé pour {dataset_name} dans {prefix}")

                insert_dataset_metric(conn, dataset_name, "Bronze", "MinIO",
                                      None, 0, False, None,
                                      f"Aucun fichier trouvé dans {prefix}")
                continue

            latest_file = max(json_objects, key=lambda obj: obj.last_modified)
            latest_update = latest_file.last_modified

            delay_minutes = (now - latest_update).total_seconds() / 60

            insert_dataset_metric(conn, dataset_name, "Bronze", "MinIO",
                                  None, file_count, True,
                                  latest_update.replace(tzinfo=None),
                                  f"Dernier fichier: {latest_file.object_name}, retard: {round(delay_minutes, 2)} minutes")

            insert_check(conn, "MinIO", "Bronze", "BRONZE_FILES_EXIST",
                         "OK", "INFO",
                         file_count, f"{file_count} fichiers trouvés pour {dataset_name}")

            freshness_status = "OK" if delay_minutes <= BRONZE_MAX_DELAY_MINUTES else "FAILED"
            freshness_severity = "INFO" if freshness_status == "OK" else "CRITICAL"

            insert_check(conn, "MinIO", "Bronze", "BRONZE_FRESHNESS",
                         freshness_status, freshness_severity,
                         round(delay_minutes, 2),
                         f"Dernier fichier Bronze {dataset_name}: {latest_file.object_name}, âge: {round(delay_minutes, 2)} minutes")

        except Exception as e:
            insert_check(conn, "MinIO", "Bronze", "BRONZE_MINIO_ACCESS",
                         "FAILED", "CRITICAL",
                         0, f"Erreur accès MinIO pour {dataset_name}: {str(e)}")


# ─── Check 2 : Datasets HDFS (Silver & Gold) ─────────────────────────────────

def check_hdfs_datasets(conn, spark):
    """
    Pour chaque dataset HDFS, vérifie :
      - l'existence du répertoire
      - la présence du fichier _SUCCESS (indique que le job Spark s'est terminé proprement)
      - le nombre de fichiers Parquet
      - le nombre de lignes (lecture réelle via Spark)
    """
    fs, jvm = get_hdfs_filesystem(spark)

    for dataset_name, layer_name, path in HDFS_DATASETS:
        full_path = f"{HDFS_URI}{path}"
        exists = hdfs_exists(fs, jvm, full_path)
        # _SUCCESS est écrit par Spark à la fin d'un saveAsParquet réussi
        success_exists = hdfs_exists(fs, jvm, f"{full_path}/_SUCCESS") if exists else False
        file_count = hdfs_count_files(fs, jvm, full_path) if exists else 0
        last_update = hdfs_last_update(fs, jvm, full_path) if exists else None
        # count() déclenche un job Spark distribué → valeur exacte même sur gros volumes
        row_count = spark.read.parquet(full_path).count() if exists else 0

        insert_dataset_metric(conn, dataset_name, layer_name, "HDFS",
                              row_count, file_count, success_exists, last_update, full_path)

        insert_check(conn, "HDFS", layer_name, f"{dataset_name}_PATH_EXISTS",
                     "OK" if exists else "FAILED",
                     "INFO" if exists else "CRITICAL",
                     1 if exists else 0, full_path)

        insert_check(conn, "HDFS", layer_name, f"{dataset_name}_SUCCESS_FILE_EXISTS",
                     "OK" if success_exists else "FAILED",
                     "INFO" if success_exists else "CRITICAL",
                     1 if success_exists else 0, f"Fichier _SUCCESS dans {full_path}")

        insert_check(conn, "HDFS", layer_name, f"{dataset_name}_ROW_COUNT",
                     "OK" if row_count > 0 else "FAILED",
                     "INFO" if row_count > 0 else "CRITICAL",
                     row_count, f"{row_count} ligne(s) dans {dataset_name}")


# ─── Check 3 : Qualité des données Silver ────────────────────────────────────

def check_silver_data_quality(conn, spark):
    print("Checking Silver data quality...")

    silver_stations_path = f"{HDFS_URI}/data/silver/stations"
    silver_releves_path = f"{HDFS_URI}/data/silver/releves"

    try:
        stations_df = spark.read.parquet(silver_stations_path)
        releves_df = spark.read.parquet(silver_releves_path)

        stations_count = stations_df.count()
        releves_count = releves_df.count()

        insert_check(conn, "Data Quality", "Silver", "SILVER_STATIONS_NOT_EMPTY",
                     "OK" if stations_count > 0 else "FAILED",
                     "INFO" if stations_count > 0 else "CRITICAL",
                     stations_count, f"Nombre de stations Silver : {stations_count}")

        insert_check(conn, "Data Quality", "Silver", "SILVER_RELEVES_NOT_EMPTY",
                     "OK" if releves_count > 0 else "FAILED",
                     "INFO" if releves_count > 0 else "CRITICAL",
                     releves_count, f"Nombre de relevés Silver : {releves_count}")

        if "id_station" in stations_df.columns:
            null_station_ids = stations_df.filter("id_station IS NULL").count()

            insert_check(conn, "Data Quality", "Silver", "SILVER_STATIONS_ID_NOT_NULL",
                         "OK" if null_station_ids == 0 else "FAILED",
                         "INFO" if null_station_ids == 0 else "CRITICAL",
                         null_station_ids, f"Stations avec id_station null : {null_station_ids}")

            duplicate_stations = (
                stations_df
                .groupBy("id_station")
                .count()
                .filter("count > 1")
                .count()
            )

            insert_check(conn, "Data Quality", "Silver", "SILVER_STATIONS_ID_UNIQUE",
                         "OK" if duplicate_stations == 0 else "FAILED",
                         "INFO" if duplicate_stations == 0 else "CRITICAL",
                         duplicate_stations,
                         f"Nombre d'id_station en doublon dans Silver stations : {duplicate_stations}")

        if "id_station" in releves_df.columns:
            null_releve_ids = releves_df.filter("id_station IS NULL").count()

            insert_check(conn, "Data Quality", "Silver", "SILVER_RELEVES_ID_NOT_NULL",
                         "OK" if null_releve_ids == 0 else "FAILED",
                         "INFO" if null_releve_ids == 0 else "CRITICAL",
                         null_releve_ids, f"Relevés avec id_station null : {null_releve_ids}")

        if "date_collecte" in releves_df.columns:
            null_dates = releves_df.filter("date_collecte IS NULL").count()

            insert_check(conn, "Data Quality", "Silver", "SILVER_RELEVES_DATE_NOT_NULL",
                         "OK" if null_dates == 0 else "FAILED",
                         "INFO" if null_dates == 0 else "CRITICAL",
                         null_dates, f"Relevés avec date_collecte null : {null_dates}")

        numeric_columns_to_check = [
            "velos_disponibles",
            "velos_mecaniques",
            "velos_electriques",
            "places_disponibles",
            "bornes_disponibles",
            "capacite",
        ]

        existing_numeric_columns = [
            col_name for col_name in numeric_columns_to_check
            if col_name in releves_df.columns
        ]

        negative_total = sum(
            releves_df.filter(f"{col_name} < 0").count()
            for col_name in existing_numeric_columns
        )

        insert_check(conn, "Data Quality", "Silver", "SILVER_NO_NEGATIVE_VALUES",
                     "OK" if negative_total == 0 else "FAILED",
                     "INFO" if negative_total == 0 else "CRITICAL",
                     negative_total,
                     f"Nombre de valeurs négatives détectées dans Silver relevés : {negative_total}")

        if "id_station" in stations_df.columns and "id_station" in releves_df.columns:
            unknown_station_count = (
                releves_df.select("id_station").distinct()
                .join(stations_df.select("id_station").distinct(), on="id_station", how="left_anti")
                .count()
            )

            insert_check(conn, "Data Quality", "Silver", "SILVER_RELEVES_STATIONS_RELATION",
                         "OK" if unknown_station_count == 0 else "FAILED",
                         "INFO" if unknown_station_count == 0 else "CRITICAL",
                         unknown_station_count,
                         f"Stations dans relevés absentes de la table stations : {unknown_station_count}")

    except Exception as e:
        insert_check(conn, "Data Quality", "Silver", "SILVER_QUALITY_CHECK_ERROR",
                     "FAILED", "CRITICAL",
                     0, f"Erreur lors des contrôles qualité Silver : {str(e)}")


# ─── Check 4 : Tables Gold PostgreSQL ────────────────────────────────────────

def get_scalar(conn, query):
    """Exécute une requête SQL et retourne la première cellule du premier résultat."""
    with conn.cursor() as cursor:
        cursor.execute(query)
        row = cursor.fetchone()
        return row[0] if row else None


def check_postgres_gold_tables(conn):
    """
    Vérifie que les tables Gold PostgreSQL ne sont pas vides.
    Ces tables sont le miroir des datasets HDFS Gold, utilisées par l'API et les dashboards.
    """
    for table_name, layer_name in POSTGRES_GOLD_TABLES:
        row_count = get_scalar(conn, f"SELECT COUNT(*) FROM {table_name};")
        insert_dataset_metric(conn, table_name, layer_name, "PostgreSQL",
                              row_count, None, None, None, f"Table PostgreSQL {table_name}")
        insert_check(conn, "PostgreSQL", layer_name, f"{table_name}_NOT_EMPTY",
                     "OK" if row_count > 0 else "FAILED",
                     "INFO" if row_count > 0 else "CRITICAL",
                     row_count, f"{row_count} ligne(s) dans {table_name}")


# ─── Check 4 : Qualité des données Gold ──────────────────────────────────────

def check_data_quality(conn):
    """
    Vérifie la cohérence métier des données Gold dans PostgreSQL.
    Ces règles sont spécifiques au domaine Vélib' et ne peuvent pas être
    inférées depuis le schéma seul.
    """

    # taux_disponibilite est un ratio [0, 1] : hors de cette plage = corruption
    invalid_rate_count = get_scalar(
        conn,
        """
        SELECT COUNT(*) FROM gold.disponibilite_courante
        WHERE taux_disponibilite IS NULL
           OR taux_disponibilite < 0
           OR taux_disponibilite > 1;
        """,
    )
    insert_check(conn, "Data Quality", "Gold", "TAUX_DISPONIBILITE_BETWEEN_0_AND_1",
                 "OK" if invalid_rate_count == 0 else "FAILED",
                 "INFO" if invalid_rate_count == 0 else "CRITICAL",
                 invalid_rate_count, f"{invalid_rate_count} ligne(s) avec taux_disponibilite invalide")

    # Des valeurs négatives indiqueraient une erreur de calcul dans le pipeline Silver→Gold
    negative_values_count = get_scalar(
        conn,
        """
        SELECT COUNT(*) FROM gold.disponibilite_courante
        WHERE velos_disponibles < 0
           OR velos_mecaniques < 0
           OR velos_electriques < 0
           OR bornes_disponibles < 0
           OR capacite < 0;
        """,
    )
    insert_check(conn, "Data Quality", "Gold", "NO_NEGATIVE_VALUES",
                 "OK" if negative_values_count == 0 else "FAILED",
                 "INFO" if negative_values_count == 0 else "CRITICAL",
                 negative_values_count, f"{negative_values_count} ligne(s) avec valeurs negatives")

    # Cohérence croisée : nombre de stations dans disponibilite_courante
    # doit égaler nombre_total_stations dans kpi_reseau_courant
    dispo_count = get_scalar(conn, "SELECT COUNT(*) FROM gold.disponibilite_courante;")
    kpi_station_count = get_scalar(conn, "SELECT nombre_total_stations FROM gold.kpi_reseau_courant LIMIT 1;")
    insert_check(conn, "Data Quality", "Gold", "KPI_STATION_COUNT_COHERENCE",
                 "OK" if dispo_count == kpi_station_count else "FAILED",
                 "INFO" if dispo_count == kpi_station_count else "CRITICAL",
                 dispo_count,
                 f"disponibilite_courante={dispo_count}, kpi_reseau_courant={kpi_station_count}")

    # date_generation NULL = le KPI courant a été inséré sans horodatage → non traçable
    missing_date_generation_count = get_scalar(
        conn,
        "SELECT COUNT(*) FROM gold.kpi_reseau_courant WHERE date_generation IS NULL;",
    )
    insert_check(conn, "Data Quality", "Gold", "KPI_CURRENT_DATE_GENERATION_NOT_NULL",
                 "OK" if missing_date_generation_count == 0 else "FAILED",
                 "INFO" if missing_date_generation_count == 0 else "CRITICAL",
                 missing_date_generation_count,
                 f"{missing_date_generation_count} KPI courant sans date_generation")

    # L'historique doit contenir au moins un enregistrement après le premier run Gold
    kpi_history_count = get_scalar(conn, "SELECT COUNT(*) FROM gold.kpi_reseau_historique;")
    insert_check(conn, "Data Quality", "Gold", "KPI_HISTORY_NOT_EMPTY",
                 "OK" if kpi_history_count > 0 else "FAILED",
                 "INFO" if kpi_history_count > 0 else "CRITICAL",
                 kpi_history_count, f"{kpi_history_count} ligne(s) dans gold.kpi_reseau_historique")


# ─── Point d'entrée ───────────────────────────────────────────────────────────

def main():
    spark = None
    conn = None
    try:
        conn = get_postgres_connection()
        ensure_monitoring_tables(conn)   # idempotent : CREATE IF NOT EXISTS
        spark = create_spark_session()

        check_hdfs_cluster(conn)         # 1. Santé du cluster HDFS
        check_bronze_minio(conn)         # 2. Datasets Bronze MinIO
        check_hdfs_datasets(conn, spark)        # 3. Présence et contenu des datasets HDFS
        check_silver_data_quality(conn, spark)  # 4. Qualité des données Silver
        check_postgres_gold_tables(conn)        # 5. Tables Gold PostgreSQL non vides
        check_data_quality(conn)                # 6. Règles métier Vélib' Gold

        # Marqueur de fin d'exécution : utile pour détecter un run incomplet
        insert_check(conn, "Monitoring", "Global", "MONITORING_RUN_COMPLETED",
                     "OK", "INFO", 1, "Execution du monitoring terminee avec succes")
        print("Monitoring termine avec succes.")
    finally:
        # finally garantit l'arrêt de Spark et la fermeture de la connexion
        # même en cas d'exception non gérée
        if spark:
            spark.stop()
        if conn:
            conn.close()


if __name__ == "__main__":
    main()

from pyspark.sql import SparkSession, Window
from pyspark import StorageLevel
from pyspark.sql.functions import (
    col,
    lit,
    when,
    row_number,
    hour,
    avg,
    count,
    sum as spark_sum,
    round as spark_round,
    max as spark_max,
    least,
    current_timestamp,
)

import psycopg2
from psycopg2.extras import execute_values

from config import (
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PORT,
)

from quality.ge_validations import validate_gold


SILVER_STATIONS_PATH = "hdfs://hdfs-namenode:9000/data/silver/stations"
SILVER_RELEVES_PATH = "hdfs://hdfs-namenode:9000/data/silver/releves"

GOLD_DISPO_PATH = "hdfs://hdfs-namenode:9000/data/gold/disponibilite_courante"
GOLD_TENDANCE_PATH = "hdfs://hdfs-namenode:9000/data/gold/tendance_horaire"
GOLD_KPI_PATH = "hdfs://hdfs-namenode:9000/data/gold/kpi_reseau_courant"
GOLD_KPI_HISTORY_PATH = "hdfs://hdfs-namenode:9000/data/gold/kpi_reseau_historique"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("velib_silver_to_gold")
        .master("spark://spark-master:7077")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000")
        .config("spark.hadoop.dfs.replication", "2")
        .getOrCreate()
    )
def mettre_en_cache(df, nom_dataset):
    """
    Met en cache un DataFrame avant insertion HDFS/PostgreSQL.
    MEMORY_AND_DISK permet de garder les données en mémoire et de déborder sur disque si nécessaire.
    """
    df_cache = df.persist(StorageLevel.MEMORY_AND_DISK)
    nb_lignes = df_cache.count()
    print(f"[CACHE] {nom_dataset} mis en cache avant insertion - {nb_lignes} lignes")
    return df_cache

def get_postgres_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def clean_value(value):
    if value is None:
        return None
    return value


def ensure_gold_tables(conn):
    with conn.cursor() as cursor:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS gold;")

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS gold.disponibilite_courante (
                id_station INTEGER PRIMARY KEY,
                nom TEXT,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                capacite INTEGER,
                velos_disponibles INTEGER,
                bornes_disponibles INTEGER,
                velos_mecaniques INTEGER,
                velos_electriques INTEGER,
                taux_disponibilite NUMERIC,
                est_vide BOOLEAN,
                est_pleine BOOLEAN,
                derniere_collecte TIMESTAMP
            );
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS gold.tendance_horaire (
                id_station INTEGER,
                nom TEXT,
                heure SMALLINT,
                moy_velos_disponibles NUMERIC,
                moy_taux_disponibilite NUMERIC,
                nb_fois_vide INTEGER,
                nb_observations INTEGER
            );
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS gold.kpi_reseau_courant (
                id SMALLINT PRIMARY KEY,
                date_generation TIMESTAMP,
                derniere_collecte TIMESTAMP,
                nombre_total_stations INTEGER,
                nombre_stations_vides INTEGER,
                nombre_stations_pleines INTEGER,
                nombre_stations_en_tension INTEGER,
                total_velos_disponibles INTEGER,
                total_velos_mecaniques INTEGER,
                total_velos_electriques INTEGER,
                total_bornes_disponibles INTEGER,
                taux_disponibilite_global NUMERIC
            );
            """
        )

        cursor.execute(
            "ALTER TABLE gold.kpi_reseau_courant ADD COLUMN IF NOT EXISTS date_generation TIMESTAMP;"
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS gold.kpi_reseau_historique (
                id BIGSERIAL PRIMARY KEY,
                date_generation TIMESTAMP,
                derniere_collecte TIMESTAMP,
                nombre_total_stations INTEGER,
                nombre_stations_vides INTEGER,
                nombre_stations_pleines INTEGER,
                nombre_stations_en_tension INTEGER,
                total_velos_disponibles INTEGER,
                total_velos_mecaniques INTEGER,
                total_velos_electriques INTEGER,
                total_bornes_disponibles INTEGER,
                taux_disponibilite_global NUMERIC
            );
            """
        )

    conn.commit()


def build_disponibilite_courante(df_stations, df_releves):
    window_latest = Window.partitionBy("id_station").orderBy(col("date_collecte").desc())

    df_latest = (
        df_releves
        .withColumn("rang", row_number().over(window_latest))
        .filter(col("rang") == 1)
        .drop("rang")
    )

    df_gold = (
        df_latest.alias("r")
        .join(
            df_stations.alias("s"),
            col("r.id_station") == col("s.id_station"),
            "inner",
        )
        .select(
            col("r.id_station").alias("id_station"),
            col("s.nom").alias("nom"),
            col("s.latitude").alias("latitude"),
            col("s.longitude").alias("longitude"),
            col("s.capacite").alias("capacite"),
            col("r.velos_disponibles").alias("velos_disponibles"),
            col("r.bornes_disponibles").alias("bornes_disponibles"),
            col("r.velos_mecaniques").alias("velos_mecaniques"),
            col("r.velos_electriques").alias("velos_electriques"),
            when(
                col("s.capacite") > 0,
                least(
                    spark_round(col("r.velos_disponibles") / col("s.capacite"), 3),
                    lit(1.0),
                ),
            )
            .otherwise(lit(0.0))
            .alias("taux_disponibilite"),
            (col("r.velos_disponibles") == 0).alias("est_vide"),
            (col("r.bornes_disponibles") == 0).alias("est_pleine"),
            col("r.date_collecte").alias("derniere_collecte"),
        )
    )

    return df_gold


def build_tendance_horaire(df_stations, df_releves):
    df_joined = (
        df_releves.alias("r")
        .join(
            df_stations.alias("s"),
            col("r.id_station") == col("s.id_station"),
            "inner",
        )
        .withColumn("heure", hour(col("r.date_collecte")).cast("int"))
    )

    df_gold = (
        df_joined
        .groupBy(
            col("r.id_station").alias("id_station"),
            col("s.nom").alias("nom"),
            col("heure"),
        )
        .agg(
            spark_round(avg(col("r.velos_disponibles")), 2).alias("moy_velos_disponibles"),
            spark_round(
                avg(
                    when(
                        col("s.capacite") > 0,
                        col("r.velos_disponibles") / col("s.capacite"),
                    ).otherwise(lit(0.0))
                ),
                3,
            ).alias("moy_taux_disponibilite"),
            spark_sum(
                when(col("r.velos_disponibles") == 0, lit(1)).otherwise(lit(0))
            ).alias("nb_fois_vide"),
            count(lit(1)).alias("nb_observations"),
        )
        .orderBy("id_station", "heure")
    )

    return df_gold


def build_kpi_reseau_courant(df_dispo):
    df_kpi = (
        df_dispo
        .agg(
            spark_max(col("derniere_collecte")).alias("derniere_collecte"),
            count(lit(1)).alias("nombre_total_stations"),
            spark_sum(
                when(col("est_vide") == True, lit(1)).otherwise(lit(0))
            ).alias("nombre_stations_vides"),
            spark_sum(
                when(col("est_pleine") == True, lit(1)).otherwise(lit(0))
            ).alias("nombre_stations_pleines"),
            spark_sum(
                when(col("taux_disponibilite") < 0.2, lit(1)).otherwise(lit(0))
            ).alias("nombre_stations_en_tension"),
            spark_sum(col("velos_disponibles")).alias("total_velos_disponibles"),
            spark_sum(col("velos_mecaniques")).alias("total_velos_mecaniques"),
            spark_sum(col("velos_electriques")).alias("total_velos_electriques"),
            spark_sum(col("bornes_disponibles")).alias("total_bornes_disponibles"),
            least(
                spark_round(
                    spark_sum(col("velos_disponibles")) / spark_sum(col("capacite")),
                    3,
                ),
                lit(1.0),
            ).alias("taux_disponibilite_global"),
        )
        .withColumn("id", lit(1).cast("smallint"))
        .withColumn("date_generation", current_timestamp())
        .select(
            "id",
            "date_generation",
            "derniere_collecte",
            "nombre_total_stations",
            "nombre_stations_vides",
            "nombre_stations_pleines",
            "nombre_stations_en_tension",
            "total_velos_disponibles",
            "total_velos_mecaniques",
            "total_velos_electriques",
            "total_bornes_disponibles",
            "taux_disponibilite_global",
        )
    )

    return df_kpi


def load_disponibilite_courante_to_postgres(conn, df):
    rows = df.collect()

    values = [
        (
            clean_value(row["id_station"]),
            clean_value(row["nom"]),
            clean_value(row["latitude"]),
            clean_value(row["longitude"]),
            clean_value(row["capacite"]),
            clean_value(row["velos_disponibles"]),
            clean_value(row["bornes_disponibles"]),
            clean_value(row["velos_mecaniques"]),
            clean_value(row["velos_electriques"]),
            clean_value(row["taux_disponibilite"]),
            clean_value(row["est_vide"]),
            clean_value(row["est_pleine"]),
            clean_value(row["derniere_collecte"]),
        )
        for row in rows
    ]

    query = """
        INSERT INTO gold.disponibilite_courante (
            id_station,
            nom,
            latitude,
            longitude,
            capacite,
            velos_disponibles,
            bornes_disponibles,
            velos_mecaniques,
            velos_electriques,
            taux_disponibilite,
            est_vide,
            est_pleine,
            derniere_collecte
        )
        VALUES %s
        ON CONFLICT (id_station)
        DO UPDATE SET
            nom                 = EXCLUDED.nom,
            latitude            = EXCLUDED.latitude,
            longitude           = EXCLUDED.longitude,
            capacite            = EXCLUDED.capacite,
            velos_disponibles   = EXCLUDED.velos_disponibles,
            bornes_disponibles  = EXCLUDED.bornes_disponibles,
            velos_mecaniques    = EXCLUDED.velos_mecaniques,
            velos_electriques   = EXCLUDED.velos_electriques,
            taux_disponibilite  = EXCLUDED.taux_disponibilite,
            est_vide            = EXCLUDED.est_vide,
            est_pleine          = EXCLUDED.est_pleine,
            derniere_collecte   = EXCLUDED.derniere_collecte;
    """

    with conn.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE gold.disponibilite_courante;")
        execute_values(cursor, query, values)

    conn.commit()
    print(f"gold.disponibilite_courante mise a jour : {len(values)} lignes")


def load_tendance_horaire_to_postgres(conn, df):
    rows = df.collect()

    values = [
        (
            clean_value(row["id_station"]),
            clean_value(row["nom"]),
            int(row["heure"]),
            clean_value(row["moy_velos_disponibles"]),
            clean_value(row["moy_taux_disponibilite"]),
            int(row["nb_fois_vide"]),
            int(row["nb_observations"]),
        )
        for row in rows
    ]

    with conn.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE gold.tendance_horaire;")

        execute_values(
            cursor,
            """
            INSERT INTO gold.tendance_horaire (
                id_station,
                nom,
                heure,
                moy_velos_disponibles,
                moy_taux_disponibilite,
                nb_fois_vide,
                nb_observations
            )
            VALUES %s
            """,
            values,
        )

    conn.commit()
    print(f"gold.tendance_horaire recalculee : {len(values)} lignes")


def load_kpi_reseau_courant_to_postgres(conn, df):
    rows = df.collect()

    values = [
        (
            int(row["id"]),
            clean_value(row["date_generation"]),
            clean_value(row["derniere_collecte"]),
            clean_value(row["nombre_total_stations"]),
            clean_value(row["nombre_stations_vides"]),
            clean_value(row["nombre_stations_pleines"]),
            clean_value(row["nombre_stations_en_tension"]),
            clean_value(row["total_velos_disponibles"]),
            clean_value(row["total_velos_mecaniques"]),
            clean_value(row["total_velos_electriques"]),
            clean_value(row["total_bornes_disponibles"]),
            clean_value(row["taux_disponibilite_global"]),
        )
        for row in rows
    ]

    query = """
        INSERT INTO gold.kpi_reseau_courant (
            id,
            date_generation,
            derniere_collecte,
            nombre_total_stations,
            nombre_stations_vides,
            nombre_stations_pleines,
            nombre_stations_en_tension,
            total_velos_disponibles,
            total_velos_mecaniques,
            total_velos_electriques,
            total_bornes_disponibles,
            taux_disponibilite_global
        )
        VALUES %s
        ON CONFLICT (id)
        DO UPDATE SET
            date_generation             = EXCLUDED.date_generation,
            derniere_collecte           = EXCLUDED.derniere_collecte,
            nombre_total_stations       = EXCLUDED.nombre_total_stations,
            nombre_stations_vides       = EXCLUDED.nombre_stations_vides,
            nombre_stations_pleines     = EXCLUDED.nombre_stations_pleines,
            nombre_stations_en_tension  = EXCLUDED.nombre_stations_en_tension,
            total_velos_disponibles     = EXCLUDED.total_velos_disponibles,
            total_velos_mecaniques      = EXCLUDED.total_velos_mecaniques,
            total_velos_electriques     = EXCLUDED.total_velos_electriques,
            total_bornes_disponibles    = EXCLUDED.total_bornes_disponibles,
            taux_disponibilite_global   = EXCLUDED.taux_disponibilite_global;
    """

    with conn.cursor() as cursor:
        execute_values(cursor, query, values)

    conn.commit()
    print("gold.kpi_reseau_courant mis a jour")


def load_kpi_reseau_historique_to_postgres(conn, df_kpi):
    rows = df_kpi.collect()

    values = [
        (
            row["date_generation"],
            row["derniere_collecte"],
            row["nombre_total_stations"],
            row["nombre_stations_vides"],
            row["nombre_stations_pleines"],
            row["nombre_stations_en_tension"],
            row["total_velos_disponibles"],
            row["total_velos_mecaniques"],
            row["total_velos_electriques"],
            row["total_bornes_disponibles"],
            row["taux_disponibilite_global"],
        )
        for row in rows
    ]

    query = """
        INSERT INTO gold.kpi_reseau_historique (
            date_generation,
            derniere_collecte,
            nombre_total_stations,
            nombre_stations_vides,
            nombre_stations_pleines,
            nombre_stations_en_tension,
            total_velos_disponibles,
            total_velos_mecaniques,
            total_velos_electriques,
            total_bornes_disponibles,
            taux_disponibilite_global
        )
        VALUES %s
    """

    with conn.cursor() as cursor:
        execute_values(cursor, query, values)

    conn.commit()
    print(f"gold.kpi_reseau_historique alimentee : {len(values)} ligne(s)")


def run_silver_to_gold_spark():
    print("Debut transformation Silver HDFS vers Gold")

    spark = create_spark_session()

    try:
        print("Lecture Silver depuis HDFS...")
        df_stations = spark.read.parquet(SILVER_STATIONS_PATH)
        df_releves = spark.read.parquet(SILVER_RELEVES_PATH)

        print("Construction Gold disponibilite_courante...")
        df_dispo = mettre_en_cache(
            build_disponibilite_courante(df_stations, df_releves),
            "gold.disponibilite_courante",
        )

        print("Construction Gold tendance_horaire...")
        df_tendance = mettre_en_cache(
            build_tendance_horaire(df_stations, df_releves),
            "gold.tendance_horaire",
        )

        print("Construction Gold kpi_reseau_courant...")
        df_kpi = mettre_en_cache(
            build_kpi_reseau_courant(df_dispo),
            "gold.kpi_reseau_courant",
        )

        conn = get_postgres_connection()

        try:
            ensure_gold_tables(conn)

            # disponibilite_courante
            df_dispo.repartition(4).write.mode("overwrite").parquet(GOLD_DISPO_PATH)
            load_disponibilite_courante_to_postgres(conn, df_dispo)
            df_dispo.unpersist()
            print("[CACHE] gold.disponibilite_courante libere du cache")

            # tendance_horaire
            df_tendance.repartition(4).write.mode("overwrite").parquet(GOLD_TENDANCE_PATH)
            load_tendance_horaire_to_postgres(conn, df_tendance)
            df_tendance.unpersist()
            print("[CACHE] gold.tendance_horaire libere du cache")

            # kpi_reseau_courant + kpi_reseau_historique
            df_kpi.coalesce(1).write.mode("overwrite").parquet(GOLD_KPI_PATH)
            df_kpi.coalesce(1).write.mode("append").parquet(GOLD_KPI_HISTORY_PATH)
            load_kpi_reseau_courant_to_postgres(conn, df_kpi)
            load_kpi_reseau_historique_to_postgres(conn, df_kpi)
            df_kpi.unpersist()
            print("[CACHE] gold.kpi_reseau_courant/historique libere du cache")

        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        print("Transformation Silver vers Gold terminee avec succes")

    finally:
        spark.stop()

    try:
        validate_gold()
    except Exception as e:
        print(f"Validation Gold non bloquante en erreur : {e}")


if __name__ == "__main__":
    run_silver_to_gold_spark()
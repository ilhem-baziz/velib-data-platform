import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, from_unixtime, to_timestamp, expr
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from config import MINIO_BUCKET
from utils.minio_client import get_minio_client

SILVER_STATIONS_PATH = "hdfs://hdfs-namenode:9000/data/silver/stations"
SILVER_RELEVES_PATH = "hdfs://hdfs-namenode:9000/data/silver/releves"
# ============================================================
# Spark session
# ============================================================

def create_spark_session():
    return (
        SparkSession.builder
        .appName("velib_bronze_to_silver")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.dfs.replication", "2")
        .getOrCreate()
    )


# ============================================================
# Schemas proches de l'API Vélib
# ============================================================

STATION_SCHEMA = StructType([
    StructField("station_id", StringType(), True),
    StructField("stationCode", StringType(), True),
    StructField("name", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("capacity", IntegerType(), True),
    StructField("station_opening_hours", StringType(), True),
])

BIKE_TYPE_SCHEMA = ArrayType(
    StructType([
        StructField("mechanical", IntegerType(), True),
        StructField("ebike", IntegerType(), True),
    ])
)

RELEVE_SCHEMA = StructType([
    StructField("station_id", StringType(), True),
    StructField("stationCode", StringType(), True),
    StructField("num_bikes_available", IntegerType(), True),
    StructField("num_docks_available", IntegerType(), True),
    StructField("num_bikes_available_types", BIKE_TYPE_SCHEMA, True),
    StructField("is_installed", IntegerType(), True),
    StructField("is_returning", IntegerType(), True),
    StructField("is_renting", IntegerType(), True),
    StructField("last_reported", LongType(), True),
])


# ============================================================
# Helpers de nettoyage avant création des DataFrames Spark
# ============================================================

def safe_str(value):
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def safe_int(value):
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def safe_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def safe_long(value):
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def clean_bike_types(value):
    """
    L'API renvoie souvent :
    [
      {"mechanical": 3},
      {"ebike": 5}
    ]

    On force une structure propre compatible avec le schema Spark.
    """
    if value is None:
        return []

    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            return []

    if not isinstance(value, list):
        return []

    cleaned = []

    for item in value:
        if isinstance(item, dict):
            cleaned.append(
                {
                    "mechanical": safe_int(item.get("mechanical")) or 0,
                    "ebike": safe_int(item.get("ebike")) or 0,
                }
            )

    return cleaned


# ============================================================
# Lecture du dernier fichier JSON dans MinIO
# ============================================================

def get_latest_json_from_minio(prefix):
    client = get_minio_client()

    objects = list(
        client.list_objects(
            MINIO_BUCKET,
            prefix=prefix,
            recursive=True,
        )
    )

    json_objects = [
        obj for obj in objects
        if obj.object_name.endswith(".json")
    ]

    if not json_objects:
        raise FileNotFoundError(f"Aucun fichier JSON trouve dans MinIO : {prefix}")

    latest_object = max(json_objects, key=lambda obj: obj.last_modified)

    response = client.get_object(MINIO_BUCKET, latest_object.object_name)

    try:
        data = json.loads(response.read().decode("utf-8"))
    finally:
        response.close()
        response.release_conn()

    print(f"Fichier Bronze lu : {latest_object.object_name}")

    return data, latest_object.object_name


# ============================================================
# Préparation des données stations
# ============================================================

def prepare_station_rows(station_information_json):
    stations = station_information_json["data"]["stations"]

    cleaned = []

    for station in stations:
        cleaned.append(
            {
                "station_id": safe_str(station.get("station_id")),
                "stationCode": safe_str(station.get("stationCode")),
                "name": safe_str(station.get("name")),
                "lat": safe_float(station.get("lat")),
                "lon": safe_float(station.get("lon")),
                "capacity": safe_int(station.get("capacity")),
                "station_opening_hours": safe_str(
                    station.get("station_opening_hours")
                ),
            }
        )

    return cleaned


def transform_stations_spark(spark, station_information_json):
    station_rows = prepare_station_rows(station_information_json)

    df = spark.createDataFrame(station_rows, schema=STATION_SCHEMA)

    df = (
        df.select(
            col("station_id").alias("id_station"),
            col("stationCode").alias("code_station"),
            col("name").alias("nom"),
            col("lat").alias("latitude"),
            col("lon").alias("longitude"),
            col("capacity").alias("capacite"),
            col("station_opening_hours").alias("horaires_ouverture"),
        )
        .withColumn("id_station", col("id_station").cast("int"))
        .withColumn("code_station", col("code_station").cast("string"))
        .withColumn("nom", col("nom").cast("string"))
        .withColumn("latitude", col("latitude").cast("double"))
        .withColumn("longitude", col("longitude").cast("double"))
        .withColumn("capacite", col("capacite").cast("int"))
        .withColumn("horaires_ouverture", col("horaires_ouverture").cast("string"))
        .fillna({"capacite": 0})
        .dropna(subset=["id_station", "nom"])
        .dropDuplicates(["id_station"])
        .repartition(4)
    )

    return df


# ============================================================
# Préparation des relevés station_status
# ============================================================

def prepare_releve_rows(station_status_json):
    releves = station_status_json["data"]["stations"]

    cleaned = []

    for releve in releves:
        cleaned.append(
            {
                "station_id": safe_str(releve.get("station_id")),
                "stationCode": safe_str(releve.get("stationCode")),
                "num_bikes_available": safe_int(
                    releve.get("num_bikes_available")
                ),
                "num_docks_available": safe_int(
                    releve.get("num_docks_available")
                ),
                "num_bikes_available_types": clean_bike_types(
                    releve.get("num_bikes_available_types")
                ),
                "is_installed": safe_int(releve.get("is_installed")),
                "is_returning": safe_int(releve.get("is_returning")),
                "is_renting": safe_int(releve.get("is_renting")),
                "last_reported": safe_long(releve.get("last_reported")),
            }
        )

    return cleaned



def transform_releves_spark(spark, station_status_json, source_file_status):
    releve_rows = prepare_releve_rows(station_status_json)

    df = spark.createDataFrame(releve_rows, schema=RELEVE_SCHEMA)

    date_collecte = datetime.now(timezone.utc).replace(microsecond=0)
    date_collecte_str = date_collecte.strftime("%Y-%m-%d %H:%M:%S")

    df = (
        df.withColumn(
            "velos_mecaniques",
            expr(
                "aggregate(num_bikes_available_types, 0, "
                "(acc, x) -> acc + coalesce(x.mechanical, 0))"
            ),
        )
        .withColumn(
            "velos_electriques",
            expr(
                "aggregate(num_bikes_available_types, 0, "
                "(acc, x) -> acc + coalesce(x.ebike, 0))"
            ),
        )
        .withColumn("date_collecte", to_timestamp(lit(date_collecte_str)))
        .withColumn("source_file", lit(source_file_status))
        .select(
            col("station_id").alias("id_station"),
            col("stationCode").alias("code_station"),
            col("date_collecte"),
            col("num_bikes_available").alias("velos_disponibles"),
            col("num_docks_available").alias("bornes_disponibles"),
            col("velos_mecaniques"),
            col("velos_electriques"),
            col("is_installed").alias("est_installee"),
            col("is_returning").alias("retour_possible"),
            col("is_renting").alias("location_possible"),
            col("last_reported").alias("dernier_signalement"),
            col("source_file"),
        )
        .withColumn("id_station", col("id_station").cast("int"))
        .withColumn("code_station", col("code_station").cast("string"))
        .withColumn("velos_disponibles", col("velos_disponibles").cast("int"))
        .withColumn("bornes_disponibles", col("bornes_disponibles").cast("int"))
        .withColumn("velos_mecaniques", col("velos_mecaniques").cast("int"))
        .withColumn("velos_electriques", col("velos_electriques").cast("int"))
        .withColumn("est_installee", col("est_installee").cast("int"))
        .withColumn("retour_possible", col("retour_possible").cast("int"))
        .withColumn("location_possible", col("location_possible").cast("int"))
        .withColumn(
            "dernier_signalement",
            to_timestamp(from_unixtime(col("dernier_signalement"))),
        )
        .dropna(subset=["id_station", "date_collecte"])
        .fillna(
            {
                "velos_disponibles": 0,
                "bornes_disponibles": 0,
                "velos_mecaniques": 0,
                "velos_electriques": 0,
                "est_installee": 0,
                "retour_possible": 0,
                "location_possible": 0,
            }
        )
        .dropDuplicates(["id_station", "date_collecte"])
        .repartition(4)
    )

    return df


# ============================================================
# Main
# ============================================================

def run_bronze_to_silver_spark():
    print("Debut transformation Bronze vers Silver avec Spark")

    spark = create_spark_session()

    try:
        station_information_json, source_file_station = get_latest_json_from_minio(
            "bronze/station_information/"
        )

        station_status_json, source_file_status = get_latest_json_from_minio(
            "bronze/station_status/"
        )

        print(f"Source station_information : {source_file_station}")
        print(f"Source station_status      : {source_file_status}")

        df_station = transform_stations_spark(spark, station_information_json)
        df_releve = transform_releves_spark(
            spark,
            station_status_json,
            source_file_status,
        )

        print("Partitions stations :", df_station.rdd.getNumPartitions())
        print("Partitions releves  :", df_releve.rdd.getNumPartitions())

        nb_stations = df_station.count()
        nb_releves = df_releve.count()

        print("Nombre de stations transformees :", nb_stations)
        print("Nombre de releves transformes   :", nb_releves)

        # Stations = données quasi statiques, on remplace la version silver
        df_station.write.mode("overwrite").parquet(
            SILVER_STATIONS_PATH
        )

        # Releves = données historiques, on ajoute les nouveaux relevés
        df_releve.write.mode("append").parquet(
              SILVER_RELEVES_PATH
        )

        print("Ecriture Parquet terminee :")
        print("- " + SILVER_STATIONS_PATH)
        print("- " + SILVER_RELEVES_PATH)
        print("Transformation Bronze vers Silver Spark terminee avec succes")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_bronze_to_silver_spark()
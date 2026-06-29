import json
from datetime import datetime, timezone

import pandas as pd

from config import MINIO_BUCKET
from utils.minio_client import get_minio_client
from quality.quality_checks import (
    check_station_quality,
    check_releve_quality,
    check_relation_station_releve,
)
from quality.ge_validations import (
    validate_bronze,
    validate_silver_station,
    validate_silver_releve,
)
from load.load_to_postgres import (
    load_stations_to_postgres,
    load_releves_to_postgres,
)


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

    return data


def extract_bike_types(bike_types):
    velos_mecaniques = 0
    velos_electriques = 0

    if isinstance(bike_types, list):
        for item in bike_types:
            if isinstance(item, dict):
                velos_mecaniques += int(item.get("mechanical", 0) or 0)
                velos_electriques += int(item.get("ebike", 0) or 0)

    return velos_mecaniques, velos_electriques


def transform_stations(station_information_json):
    stations = station_information_json["data"]["stations"]

    df = pd.DataFrame(stations)

    df = df[
        [
            "station_id",
            "stationCode",
            "name",
            "lat",
            "lon",
            "capacity",
            "station_opening_hours",
        ]
    ].copy()

    df = df.rename(
        columns={
            "station_id": "id_station",
            "stationCode": "code_station",
            "name": "nom",
            "lat": "latitude",
            "lon": "longitude",
            "capacity": "capacite",
            "station_opening_hours": "horaires_ouverture",
        }
    )

    df["id_station"] = pd.to_numeric(df["id_station"], errors="coerce")
    df["code_station"] = df["code_station"].astype(str)
    df["nom"] = df["nom"].astype(str)
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["capacite"] = pd.to_numeric(df["capacite"], errors="coerce").fillna(0).astype(int)

    df = df.dropna(subset=["id_station", "nom"])
    df["id_station"] = df["id_station"].astype(int)

    df = df.drop_duplicates(subset=["id_station"])

    return df


def transform_releves(station_status_json):
    stations_status = station_status_json["data"]["stations"]

    df = pd.DataFrame(stations_status)

    bike_types_df = df["num_bikes_available_types"].apply(
        lambda value: pd.Series(extract_bike_types(value))
    )

    bike_types_df.columns = ["velos_mecaniques", "velos_electriques"]

    df = pd.concat([df, bike_types_df], axis=1)

    date_collecte = datetime.now(timezone.utc).replace(microsecond=0)

    df["date_collecte"] = date_collecte

    df = df[
        [
            "station_id",
            "stationCode",
            "date_collecte",
            "num_bikes_available",
            "num_docks_available",
            "velos_mecaniques",
            "velos_electriques",
            "is_installed",
            "is_returning",
            "is_renting",
            "last_reported",
        ]
    ].copy()

    df = df.rename(
        columns={
            "station_id": "id_station",
            "stationCode": "code_station",
            "num_bikes_available": "velos_disponibles",
            "num_docks_available": "bornes_disponibles",
            "is_installed": "est_installee",
            "is_returning": "retour_possible",
            "is_renting": "location_possible",
            "last_reported": "dernier_signalement",
        }
    )

    # id_station traite separement : dropna avant fillna
    # pour eviter la corruption silencieuse en station 0
    df["id_station"] = pd.to_numeric(df["id_station"], errors="coerce")
    df["code_station"] = df["code_station"].astype(str)

    df["dernier_signalement"] = pd.to_datetime(
        df["dernier_signalement"],
        unit="s",
        utc=True,
        errors="coerce",
    )

    # On retire les lignes inexploitables AVANT tout fillna
    df = df.dropna(subset=["id_station", "date_collecte"])
    df["id_station"] = df["id_station"].astype(int)

    # fillna(0) uniquement sur les colonnes ou 0 est une valeur metier valide
    safe_to_fill_columns = [
        "velos_disponibles",
        "bornes_disponibles",
        "velos_mecaniques",
        "velos_electriques",
        "est_installee",
        "retour_possible",
        "location_possible",
    ]

    for column in safe_to_fill_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)

    df = df.drop_duplicates(subset=["id_station", "date_collecte"])

    return df


def run_bronze_to_silver():
    print("Debut transformation Bronze vers Silver")

    station_information_json = get_latest_json_from_minio(
        "bronze/station_information/"
    )

    station_status_json = get_latest_json_from_minio(
        "bronze/station_status/"
    )

    # --- VALIDATION BRONZE ---
    # Valide les donnees brutes avant toute transformation
    # Si une regle CRITIQUE echoue, le pipeline s'arrete ici
    df_status_brut = pd.DataFrame(
        station_status_json["data"]["stations"]
    )
    validate_bronze(df_status_brut)

    # --- TRANSFORMATION ---
    df_station = transform_stations(station_information_json)
    df_releve = transform_releves(station_status_json)

    print(f"Nombre de stations transformees : {len(df_station)}")
    print(f"Nombre de releves transformes : {len(df_releve)}")

    # --- VALIDATION SILVER ---
    # Valide les DataFrames transformes avant chargement dans Postgres
    # Si une regle CRITIQUE echoue, le pipeline s'arrete ici
    validate_silver_station(df_station)
    validate_silver_releve(df_releve)

    # --- CONTROLES QUALITE EXISTANTS ---
    # On garde quality_checks.py en complement de GE
    # comme filet de securite supplementaire
    check_station_quality(df_station)
    check_releve_quality(df_releve)
    check_relation_station_releve(df_station, df_releve)

    # --- CHARGEMENT ---
    load_stations_to_postgres(df_station)
    load_releves_to_postgres(df_releve)

    print("Transformation Bronze vers Silver terminee avec succes")


if __name__ == "__main__":
    run_bronze_to_silver()
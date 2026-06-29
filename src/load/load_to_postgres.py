import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from config import (
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PORT,
)


def get_postgres_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def clean_value(value):
    if pd.isna(value):
        return None
    return value


def load_stations_to_postgres(df_station):
    values = [
        (
            clean_value(row["id_station"]),
            clean_value(row["code_station"]),
            clean_value(row["nom"]),
            clean_value(row["latitude"]),
            clean_value(row["longitude"]),
            clean_value(row["capacite"]),
            clean_value(row.get("horaires_ouverture")),
        )
        for _, row in df_station.iterrows()
    ]

    query = """
        INSERT INTO silver.station (
            id_station,
            code_station,
            nom,
            latitude,
            longitude,
            capacite,
            horaires_ouverture
        )
        VALUES %s
        ON CONFLICT (id_station)
        DO UPDATE SET
            code_station = EXCLUDED.code_station,
            nom = EXCLUDED.nom,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            capacite = EXCLUDED.capacite,
            horaires_ouverture = EXCLUDED.horaires_ouverture;
    """

    conn = get_postgres_connection()

    try:
        with conn.cursor() as cursor:
            execute_values(cursor, query, values)
        conn.commit()
        print(f"Stations chargées dans PostgreSQL : {len(values)} lignes")
    finally:
        conn.close()


def load_releves_to_postgres(df_releve):
    values = [
        (
            clean_value(row["id_station"]),
            clean_value(row["code_station"]),
            clean_value(row["date_collecte"]),
            clean_value(row["velos_disponibles"]),
            clean_value(row["bornes_disponibles"]),
            clean_value(row["velos_mecaniques"]),
            clean_value(row["velos_electriques"]),
            clean_value(row["est_installee"]),
            clean_value(row["retour_possible"]),
            clean_value(row["location_possible"]),
            clean_value(row["dernier_signalement"]),
        )
        for _, row in df_releve.iterrows()
    ]

    query = """
        INSERT INTO silver.releve_disponibilite (
            id_station,
            code_station,
            date_collecte,
            velos_disponibles,
            bornes_disponibles,
            velos_mecaniques,
            velos_electriques,
            est_installee,
            retour_possible,
            location_possible,
            dernier_signalement
        )
        VALUES %s
        ON CONFLICT (id_station, date_collecte)
        DO NOTHING;
    """

    conn = get_postgres_connection()

    try:
        with conn.cursor() as cursor:
            execute_values(cursor, query, values)
        conn.commit()
        print(f"Relevés chargés dans PostgreSQL : {len(values)} lignes")
    finally:
        conn.close()
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
from quality.ge_validations import validate_gold


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


def build_disponibilite_courante(conn):
    query = """
        SELECT
            r.id_station,
            s.nom,
            s.latitude,
            s.longitude,
            s.capacite,
            r.velos_disponibles,
            r.bornes_disponibles,
            r.velos_mecaniques,
            r.velos_electriques,
            CASE
                WHEN s.capacite > 0
                THEN LEAST(ROUND(r.velos_disponibles::numeric / s.capacite, 3), 1.0)
                ELSE 0
            END AS taux_disponibilite,
            (r.velos_disponibles = 0) AS est_vide,
            (r.bornes_disponibles = 0) AS est_pleine,
            r.date_collecte AS derniere_collecte
        FROM silver.releve_disponibilite r
        INNER JOIN silver.station s ON r.id_station = s.id_station
        WHERE r.date_collecte = (
            SELECT MAX(r2.date_collecte)
            FROM silver.releve_disponibilite r2
            WHERE r2.id_station = r.id_station
        )
    """
    return pd.read_sql(query, conn)


def load_disponibilite_courante(conn, df):
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
        for _, row in df.iterrows()
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
        execute_values(cursor, query, values)
    conn.commit()
    print(f"disponibilite_courante mise a jour : {len(values)} stations")


def build_tendance_horaire(conn):
    query = """
        SELECT
            r.id_station,
            s.nom,
            EXTRACT(HOUR FROM r.date_collecte)::smallint AS heure,
            ROUND(AVG(r.velos_disponibles)::numeric, 2) AS moy_velos_disponibles,
            ROUND(
                AVG(
                    CASE
                        WHEN s.capacite > 0
                        THEN r.velos_disponibles::numeric / s.capacite
                        ELSE 0
                    END
                )::numeric, 3
            ) AS moy_taux_disponibilite,
            SUM(CASE WHEN r.velos_disponibles = 0 THEN 1 ELSE 0 END) AS nb_fois_vide,
            COUNT(*) AS nb_observations
        FROM silver.releve_disponibilite r
        INNER JOIN silver.station s ON r.id_station = s.id_station
        GROUP BY r.id_station, s.nom, EXTRACT(HOUR FROM r.date_collecte)
        ORDER BY r.id_station, heure
    """
    return pd.read_sql(query, conn)


def load_tendance_horaire(conn, df):
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
        for _, row in df.iterrows()
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
            ) VALUES %s
            """,
            values,
        )
    conn.commit()
    print(f"tendance_horaire recalculee : {len(values)} lignes")


def run_silver_to_gold():
    print("Debut transformation Silver vers Gold")

    conn = get_postgres_connection()

    try:
        print("Etape 1 : disponibilite_courante...")
        df_dispo = build_disponibilite_courante(conn)
        print(f"  {len(df_dispo)} stations lues depuis Silver")
        load_disponibilite_courante(conn, df_dispo)

        print("Etape 2 : tendance_horaire...")
        df_tendance = build_tendance_horaire(conn)
        print(f"  {len(df_tendance)} lignes calculees")
        load_tendance_horaire(conn, df_tendance)

        print("Transformation Silver vers Gold terminee avec succes")

    except Exception as e:
        conn.rollback()
        print(f"Erreur lors de la transformation Gold : {e}")
        raise

    finally:
        conn.close()

    # --- VALIDATION GOLD ---
    # Validation apres chargement — pas d'arret du pipeline si echec
    # On alerte uniquement via monitoring.data_quality_alerts
    validate_gold()


if __name__ == "__main__":
    run_silver_to_gold()
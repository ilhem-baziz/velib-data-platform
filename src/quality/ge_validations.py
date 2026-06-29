"""
ge_validations.py — compatible Great Expectations 1.18.2

Validation de la qualite des donnees a chaque etape du pipeline VelibData.
Enregistre les resultats dans monitoring.ge_validation_results pour Grafana.
"""

import logging
from datetime import datetime, timezone

import pandas as pd
import psycopg2
from psycopg2.extras import Json
import great_expectations as gx
from great_expectations.core import ExpectationSuite
from great_expectations.expectations.expectation_configuration import ExpectationConfiguration

from config import (
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PORT,
)

logger = logging.getLogger(__name__)


def get_postgres_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def save_validation_result(
    dag_id: str,
    suite_name: str,
    success: bool,
    nb_expectations: int,
    nb_successful: int,
    nb_failed: int,
    details: dict = None,
):
    """
    Enregistre le bilan d'une validation GE dans monitoring.ge_validation_results.
    Grafana lit cette table pour afficher le statut des validations en temps reel.
    """
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO monitoring.ge_validation_results (
                    run_time, dag_id, suite_name, success,
                    nb_expectations, nb_successful, nb_failed, details
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                datetime.now(timezone.utc),
                dag_id, suite_name, success,
                nb_expectations, nb_successful, nb_failed,
                Json(details) if details else None,
            ))
        conn.commit()
        logger.info(f"Resultat GE enregistre : {suite_name} — succes={success}")
    finally:
        conn.close()


def save_quality_alert(
    dag_id: str,
    suite_name: str,
    column_name: str,
    rule_name: str,
    severity: str,
    detail: str,
):
    """
    Enregistre une anomalie detectee dans monitoring.data_quality_alerts.
    Grafana lit cette table pour afficher les alertes qualite.
    Severity : INFO, WARNING, CRITICAL
    """
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO monitoring.data_quality_alerts (
                    detected_at, dag_id, suite_name,
                    column_name, rule_name, severity, detail
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                datetime.now(timezone.utc),
                dag_id, suite_name,
                column_name, rule_name, severity, detail,
            ))
        conn.commit()
        logger.warning(f"Alerte qualite enregistree : {rule_name} sur {column_name}")
    finally:
        conn.close()


def _validate_dataframe(
    df: pd.DataFrame,
    suite_name: str,
    dag_id: str,
    expectations: list,
    stop_on_critical: bool = True,
):
    """
    Valide un DataFrame pandas contre une liste d'expectations GE 1.18.2.
    
    - Enregistre le bilan dans monitoring.ge_validation_results
    - Enregistre chaque anomalie dans monitoring.data_quality_alerts
    - Leve une exception si une regle CRITIQUE echoue et stop_on_critical=True
    
    Compatible GE 1.18.2 : utilise ExpectationConfiguration depuis
    great_expectations.expectations.expectation_configuration
    """
    context = gx.get_context(mode="ephemeral")

    # Cree la suite d'expectations
    suite = ExpectationSuite(name=suite_name)
    for exp in expectations:
        suite.add_expectation_configuration(ExpectationConfiguration(
            type=exp["type"],
            kwargs=exp["kwargs"],
        ))

    # Cree une source de donnees pandas en memoire
    data_source = context.data_sources.add_pandas(name=f"{suite_name}_source")
    data_asset = data_source.add_dataframe_asset(name=f"{suite_name}_asset")
    batch_def = data_asset.add_batch_definition_whole_dataframe(
        f"{suite_name}_batch"
    )

    # Lance la validation
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})
    results = batch.validate(suite)

    # Analyse les resultats
    nb_expectations = len(results.results)
    nb_successful = sum(1 for r in results.results if r.success)
    nb_failed = nb_expectations - nb_successful

    save_validation_result(
        dag_id=dag_id,
        suite_name=suite_name,
        success=results.success,
        nb_expectations=nb_expectations,
        nb_successful=nb_successful,
        nb_failed=nb_failed,
    )

    # Analyse chaque regle en echec
    critical_failures = []
    for r in results.results:
        if not r.success:
            exp_type = r.expectation_config.type
            column = r.expectation_config.kwargs.get("column", "table")

            # Cherche la severite dans nos expectations
            severity = "WARNING"
            detail = "Anomalie detectee"
            for exp in expectations:
                if exp["type"] == exp_type:
                    action = exp.get("action_si_echec", "")
                    detail = exp.get("pourquoi", "Anomalie detectee")
                    if "CRITIQUE" in action.upper():
                        severity = "CRITICAL"
                        critical_failures.append(exp_type)
                    break

            save_quality_alert(
                dag_id=dag_id,
                suite_name=suite_name,
                column_name=column,
                rule_name=exp_type,
                severity=severity,
                detail=detail,
            )

            logger.warning(
                f"Regle echouee [{severity}] : {exp_type} sur '{column}'"
            )

    if stop_on_critical and critical_failures:
        raise ValueError(
            f"Validation GE CRITIQUE echouee pour {suite_name} : "
            f"{critical_failures}. Pipeline arrete."
        )

    logger.info(
        f"Validation {suite_name} : {nb_successful}/{nb_expectations} regles OK"
    )


# -------------------------------------------------------
# EXPECTATIONS PAR COUCHE
# -------------------------------------------------------

BRONZE_EXPECTATIONS = [
    {
        "type": "expect_column_to_exist",
        "kwargs": {"column": "station_id"},
        "pourquoi": "L'identifiant station doit exister dans le JSON brut",
        "action_si_echec": "CRITIQUE"
    },
    {
        "type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "station_id"},
        "pourquoi": "id_station ne doit jamais etre nul — sans lui le releve est inexploitable",
        "action_si_echec": "CRITIQUE"
    },
    {
        "type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "num_bikes_available"},
        "pourquoi": "velos_disponibles est l'indicateur metier central — une valeur nulle fausserait tous les calculs Gold",
        "action_si_echec": "CRITIQUE"
    },
    {
        "type": "expect_column_values_to_be_between",
        "kwargs": {"column": "num_bikes_available", "min_value": 0, "max_value": 200},
        "pourquoi": "Une valeur negative est impossible, une valeur > 200 depasse la capacite maximale connue",
        "action_si_echec": "WARNING"
    },
    {
        "type": "expect_column_values_to_be_between",
        "kwargs": {"column": "num_docks_available", "min_value": 0, "max_value": 200},
        "pourquoi": "Meme logique que num_bikes_available",
        "action_si_echec": "WARNING"
    },
    {
        "type": "expect_table_row_count_to_be_between",
        "kwargs": {"min_value": 1400, "max_value": 1600},
        "pourquoi": "Le reseau Velib compte ~1518 stations. Moins de 1400 = API incomplete, plus de 1600 = doublons",
        "action_si_echec": "CRITIQUE"
    },
]

SILVER_STATION_EXPECTATIONS = [
    {
        "type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "id_station"},
        "pourquoi": "Cle primaire — ne peut pas etre nul",
        "action_si_echec": "CRITIQUE"
    },
    {
        "type": "expect_column_values_to_be_unique",
        "kwargs": {"column": "id_station"},
        "pourquoi": "Chaque station doit avoir un identifiant unique — un doublon provoquerait un conflit PRIMARY KEY",
        "action_si_echec": "CRITIQUE"
    },
    {
        "type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "nom"},
        "pourquoi": "Le nom est affiche dans les dashboards — une valeur nulle provoque des lignes vides",
        "action_si_echec": "CRITIQUE"
    },
    {
        "type": "expect_column_values_to_be_between",
        "kwargs": {"column": "capacite", "min_value": 0, "max_value": 100},
        "pourquoi": "Capacite entre 0 et 100 bornes — protege aussi contre la division par zero en Gold",
        "action_si_echec": "WARNING"
    },
    {
        "type": "expect_column_values_to_be_between",
        "kwargs": {"column": "latitude", "min_value": 48.0, "max_value": 49.0},
        "pourquoi": "Paris est entre 48.0 et 49.0 de latitude — detecte les coordonnees GPS corrompues",
        "action_si_echec": "WARNING"
    },
    {
        "type": "expect_column_values_to_be_between",
        "kwargs": {"column": "longitude", "min_value": 2.0, "max_value": 3.0},
        "pourquoi": "Paris est entre 2.0 et 3.0 de longitude — detecte les coordonnees GPS corrompues",
        "action_si_echec": "WARNING"
    },
    {
        "type": "expect_table_row_count_to_be_between",
        "kwargs": {"min_value": 1400, "max_value": 1600},
        "pourquoi": "Nombre de stations attendu entre 1400 et 1600",
        "action_si_echec": "CRITIQUE"
    },
]

SILVER_RELEVE_EXPECTATIONS = [
    {
        "type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "id_station"},
        "pourquoi": "Sans id_station le releve ne peut pas etre rattache a une station",
        "action_si_echec": "CRITIQUE"
    },
    {
        "type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "date_collecte"},
        "pourquoi": "Sans date_collecte impossible d'historiser le releve",
        "action_si_echec": "CRITIQUE"
    },
    {
        "type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "velos_disponibles"},
        "pourquoi": "Indicateur metier central — une valeur nulle fausserait les calculs Gold",
        "action_si_echec": "CRITIQUE"
    },
    {
        "type": "expect_column_values_to_be_between",
        "kwargs": {"column": "velos_disponibles", "min_value": 0, "max_value": 200},
        "pourquoi": "Valeur negative impossible, valeur > 200 depasse la capacite maximale",
        "action_si_echec": "WARNING"
    },
    {
        "type": "expect_column_values_to_be_between",
        "kwargs": {"column": "bornes_disponibles", "min_value": 0, "max_value": 200},
        "pourquoi": "Meme logique que velos_disponibles",
        "action_si_echec": "WARNING"
    },
    {
        "type": "expect_column_values_to_be_between",
        "kwargs": {"column": "velos_mecaniques", "min_value": 0, "max_value": 200},
        "pourquoi": "Ne peut pas depasser le total de velos disponibles ni etre negatif",
        "action_si_echec": "WARNING"
    },
    {
        "type": "expect_column_values_to_be_between",
        "kwargs": {"column": "velos_electriques", "min_value": 0, "max_value": 200},
        "pourquoi": "Meme logique que velos_mecaniques",
        "action_si_echec": "WARNING"
    },
    {
        "type": "expect_column_values_to_be_in_set",
        "kwargs": {"column": "est_installee", "value_set": [0, 1]},
        "pourquoi": "Booleen encode en entier — toute autre valeur indique un probleme de parsing",
        "action_si_echec": "WARNING"
    },
    {
        "type": "expect_column_values_to_be_in_set",
        "kwargs": {"column": "retour_possible", "value_set": [0, 1]},
        "pourquoi": "Meme logique que est_installee",
        "action_si_echec": "WARNING"
    },
    {
        "type": "expect_column_values_to_be_in_set",
        "kwargs": {"column": "location_possible", "value_set": [0, 1]},
        "pourquoi": "Meme logique que est_installee",
        "action_si_echec": "WARNING"
    },
    {
        "type": "expect_table_row_count_to_be_between",
        "kwargs": {"min_value": 1400, "max_value": 1600},
        "pourquoi": "Nombre de releves par collecte attendu entre 1400 et 1600",
        "action_si_echec": "CRITIQUE"
    },
]

GOLD_EXPECTATIONS = [
    {
        "type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "id_station"},
        "pourquoi": "id_station nul en Gold signifie que le probleme n'a pas ete detecte en Silver",
        "action_si_echec": "CRITIQUE"
    },
    {
        "type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "taux_disponibilite"},
        "pourquoi": "Indicateur metier central de Gold — nul = probleme de calcul Silver->Gold",
        "action_si_echec": "CRITIQUE"
    },
    {
        "type": "expect_column_values_to_be_between",
        "kwargs": {"column": "taux_disponibilite", "min_value": 0, "max_value": 1},
        "pourquoi": "Taux entre 0 et 1 — verifie que la correction LEAST() fonctionne correctement",
        "action_si_echec": "CRITIQUE"
    },
    {
        "type": "expect_column_values_to_be_between",
        "kwargs": {"column": "velos_disponibles", "min_value": 0, "max_value": 200},
        "pourquoi": "Double verification apres transformation Silver->Gold",
        "action_si_echec": "WARNING"
    },
    {
        "type": "expect_column_values_to_be_between",
        "kwargs": {"column": "latitude", "min_value": 48.0, "max_value": 49.0},
        "pourquoi": "Coordonnees GPS intactes apres la jointure Silver->Gold",
        "action_si_echec": "WARNING"
    },
    {
        "type": "expect_column_values_to_be_between",
        "kwargs": {"column": "longitude", "min_value": 2.0, "max_value": 3.0},
        "pourquoi": "Coordonnees GPS intactes apres la jointure Silver->Gold",
        "action_si_echec": "WARNING"
    },
    {
        "type": "expect_table_row_count_to_be_between",
        "kwargs": {"min_value": 1400, "max_value": 1600},
        "pourquoi": "Gold doit contenir une ligne par station active — ni plus ni moins",
        "action_si_echec": "CRITIQUE"
    },
]


# -------------------------------------------------------
# FONCTIONS APPELEES PAR LES DAGS
# -------------------------------------------------------

def validate_bronze(df: pd.DataFrame, dag_id: str = "velib_silver_transformation"):
    """
    Valide les donnees brutes Bronze avant transformation.
    Appele dans bronze_to_silver.py juste apres la lecture du JSON MinIO.
    Si une regle CRITIQUE echoue, une exception est levee et le pipeline s'arrete.
    """
    logger.info("Validation GE Bronze...")
    _validate_dataframe(df, "bronze_suite", dag_id, BRONZE_EXPECTATIONS)


def validate_silver_station(
    df: pd.DataFrame,
    dag_id: str = "velib_silver_transformation"
):
    """
    Valide le DataFrame stations apres transformation, avant chargement Silver.
    Appele dans bronze_to_silver.py apres transform_stations().
    """
    logger.info("Validation GE Silver Station...")
    _validate_dataframe(
        df, "silver_station_suite", dag_id, SILVER_STATION_EXPECTATIONS
    )


def validate_silver_releve(
    df: pd.DataFrame,
    dag_id: str = "velib_silver_transformation"
):
    """
    Valide le DataFrame releves apres transformation, avant chargement Silver.
    Appele dans bronze_to_silver.py apres transform_releves().
    """
    logger.info("Validation GE Silver Releve...")
    _validate_dataframe(
        df, "silver_releve_suite", dag_id, SILVER_RELEVE_EXPECTATIONS
    )


def validate_gold(dag_id: str = "velib_gold_transformation"):
    """
    Valide la table gold.disponibilite_courante depuis PostgreSQL.
    Appele dans silver_to_gold.py apres le chargement Gold.
    Pas d'arret du pipeline si echec — les donnees sont deja chargees.
    On alerte uniquement via monitoring.data_quality_alerts.
    """
    logger.info("Validation GE Gold...")

    # Lit Gold depuis Postgres dans un DataFrame
    conn = get_postgres_connection()
    try:
        df_gold = pd.read_sql(
            "SELECT * FROM gold.disponibilite_courante", conn
        )
    finally:
        conn.close()

    # stop_on_critical=False : on alerte sans arreter le pipeline
    _validate_dataframe(
        df_gold,
        "gold_suite",
        dag_id,
        GOLD_EXPECTATIONS,
        stop_on_critical=False,
    )
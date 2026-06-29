-- =============================================
-- SCHEMA GOLD
-- =============================================
CREATE SCHEMA IF NOT EXISTS gold;

-- =============================================
-- TABLE 1 : disponibilite_courante
-- Snapshot du dernier relevé connu par station
-- Mise à jour à chaque cycle (UPSERT)
-- Répond à : suivi temps réel, stations vides/pleines
-- =============================================
CREATE TABLE IF NOT EXISTS gold.disponibilite_courante (
    id_station          BIGINT PRIMARY KEY,
    nom                 TEXT NOT NULL,
    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,
    capacite            INTEGER NOT NULL,

    velos_disponibles   INTEGER NOT NULL,
    bornes_disponibles  INTEGER NOT NULL,
    velos_mecaniques    INTEGER NOT NULL,
    velos_electriques   INTEGER NOT NULL,

    taux_disponibilite  NUMERIC(5, 3),
    est_vide            BOOLEAN NOT NULL,
    est_pleine          BOOLEAN NOT NULL,

    derniere_collecte   TIMESTAMPTZ NOT NULL,

    CONSTRAINT chk_gold_taux
        CHECK (taux_disponibilite >= 0 AND taux_disponibilite <= 1),
    CONSTRAINT chk_gold_velos_positifs
        CHECK (velos_disponibles >= 0),
    CONSTRAINT chk_gold_bornes_positives
        CHECK (bornes_disponibles >= 0)
);

-- =============================================
-- TABLE 2 : tendance_horaire
-- Moyennes historiques par station et par heure
-- Recalculée une fois par jour sur tout Silver
-- Répond à : anticiper les pics, zones à optimiser
-- =============================================
CREATE TABLE IF NOT EXISTS gold.tendance_horaire (
    id_station              BIGINT NOT NULL,
    nom                     TEXT NOT NULL,
    heure                   SMALLINT NOT NULL,

    moy_velos_disponibles   NUMERIC(6, 2),
    moy_taux_disponibilite  NUMERIC(5, 3),
    nb_fois_vide            INTEGER NOT NULL DEFAULT 0,
    nb_observations         INTEGER NOT NULL DEFAULT 0,

    CONSTRAINT pk_tendance_horaire
        PRIMARY KEY (id_station, heure),
    CONSTRAINT chk_heure_valide
        CHECK (heure >= 0 AND heure <= 23),
    CONSTRAINT chk_nb_observations_positif
        CHECK (nb_observations >= 0)
);

-- =============================================
-- VUE : indicateurs_reseau
-- Agrégats globaux calculés depuis disponibilite_courante
-- Recalculée à la volée à chaque requête Grafana
-- Répond à : vision macro du réseau, KPIs globaux
-- =============================================
CREATE OR REPLACE VIEW gold.indicateurs_reseau AS
SELECT
    COUNT(*)                                        AS nb_stations_total,
    SUM(CASE WHEN est_vide  THEN 1 ELSE 0 END)     AS nb_stations_vides,
    SUM(CASE WHEN est_pleine THEN 1 ELSE 0 END)    AS nb_stations_pleines,
    ROUND(AVG(taux_disponibilite)::NUMERIC, 3)     AS taux_disponibilite_moyen,
    SUM(velos_disponibles)                          AS total_velos_disponibles,
    SUM(bornes_disponibles)                         AS total_bornes_disponibles,
    MAX(derniere_collecte)                          AS derniere_mise_a_jour
FROM gold.disponibilite_courante;
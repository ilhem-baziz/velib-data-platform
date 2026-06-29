-- =============================================
-- SCHEMA MONITORING
-- =============================================
CREATE SCHEMA IF NOT EXISTS monitoring;

-- =============================================
-- TABLE 1 : pipeline_runs
-- Historique de chaque exécution de DAG
-- Alimentée par Airflow à chaque run
-- =============================================
CREATE TABLE IF NOT EXISTS monitoring.pipeline_runs (
    id                  SERIAL PRIMARY KEY,
    dag_id              VARCHAR(100) NOT NULL,
    run_id              VARCHAR(200) NOT NULL,
    start_time          TIMESTAMPTZ NOT NULL,
    end_time            TIMESTAMPTZ,
    status              VARCHAR(20) NOT NULL,
    records_processed   INTEGER,
    error_message       TEXT,
    CONSTRAINT unique_pipeline_run UNIQUE (dag_id, run_id)
);

-- =============================================
-- TABLE 2 : ge_validation_results
-- Résultats des validations Great Expectations
-- Une ligne par suite validée par run
-- =============================================
CREATE TABLE IF NOT EXISTS monitoring.ge_validation_results (
    id                  SERIAL PRIMARY KEY,
    run_time            TIMESTAMPTZ NOT NULL,
    dag_id              VARCHAR(100) NOT NULL,
    suite_name          VARCHAR(100) NOT NULL,
    success             BOOLEAN NOT NULL,
    nb_expectations     INTEGER NOT NULL,
    nb_successful       INTEGER NOT NULL,
    nb_failed           INTEGER NOT NULL,
    details             JSONB,
    rapport_url         TEXT
);

-- =============================================
-- TABLE 3 : data_quality_alerts
-- Anomalies détectées pendant les validations
-- Une ligne par anomalie
-- =============================================
CREATE TABLE IF NOT EXISTS monitoring.data_quality_alerts (
    id                  SERIAL PRIMARY KEY,
    detected_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    dag_id              VARCHAR(100) NOT NULL,
    suite_name          VARCHAR(100) NOT NULL,
    column_name         VARCHAR(100),
    rule_name           VARCHAR(200) NOT NULL,
    severity            VARCHAR(20) NOT NULL DEFAULT 'WARNING',
    detail              TEXT,
    resolved            BOOLEAN NOT NULL DEFAULT FALSE,
    resolved_at         TIMESTAMPTZ,
    CONSTRAINT chk_severity CHECK (severity IN ('INFO', 'WARNING', 'CRITICAL'))
);

-- =============================================
-- TABLE 4 : data_freshness
-- Fraîcheur des données par source
-- Mise à jour à chaque run Silver
-- =============================================
CREATE TABLE IF NOT EXISTS monitoring.data_freshness (
    id                  SERIAL PRIMARY KEY,
    checked_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_name         VARCHAR(100) NOT NULL,
    last_record_time    TIMESTAMPTZ NOT NULL,
    delay_minutes       NUMERIC(6,2) NOT NULL,
    is_fresh            BOOLEAN NOT NULL,
    CONSTRAINT chk_delay CHECK (delay_minutes >= 0)
);

-- =============================================
-- INDEX pour les requêtes Grafana
-- =============================================
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_dag_id
    ON monitoring.pipeline_runs(dag_id, start_time DESC);

CREATE INDEX IF NOT EXISTS idx_ge_results_run_time
    ON monitoring.ge_validation_results(run_time DESC);

CREATE INDEX IF NOT EXISTS idx_alerts_detected_at
    ON monitoring.data_quality_alerts(detected_at DESC, resolved);

CREATE INDEX IF NOT EXISTS idx_freshness_checked_at
    ON monitoring.data_freshness(checked_at DESC, source_name);
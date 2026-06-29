CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.station (
    id_station BIGINT PRIMARY KEY,
    code_station VARCHAR(30),
    nom TEXT NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    capacite INTEGER NOT NULL,
    horaires_ouverture TEXT,

    CONSTRAINT chk_station_capacite_positive
        CHECK (capacite >= 0)
);

CREATE TABLE IF NOT EXISTS silver.releve_disponibilite (
    id_releve BIGSERIAL PRIMARY KEY,
    id_station BIGINT NOT NULL,
    code_station VARCHAR(30),
    date_collecte TIMESTAMPTZ NOT NULL,

    velos_disponibles INTEGER NOT NULL,
    bornes_disponibles INTEGER NOT NULL,
    velos_mecaniques INTEGER NOT NULL,
    velos_electriques INTEGER NOT NULL,

    est_installee SMALLINT,
    retour_possible SMALLINT,
    location_possible SMALLINT,
    dernier_signalement TIMESTAMPTZ,

    CONSTRAINT fk_releve_station
        FOREIGN KEY (id_station)
        REFERENCES silver.station(id_station),

    CONSTRAINT chk_velos_disponibles_positifs
        CHECK (velos_disponibles >= 0),

    CONSTRAINT chk_bornes_disponibles_positives
        CHECK (bornes_disponibles >= 0),

    CONSTRAINT chk_velos_mecaniques_positifs
        CHECK (velos_mecaniques >= 0),

    CONSTRAINT chk_velos_electriques_positifs
        CHECK (velos_electriques >= 0),

    CONSTRAINT unique_releve_station_date
        UNIQUE (id_station, date_collecte)
);
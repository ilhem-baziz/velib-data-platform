def check_station_quality(df_station):
    """
    Vérifie la qualité des données de la table station.
    Si une règle critique échoue, le pipeline s'arrête.
    """

    required_columns = [
        "id_station",
        "code_station",
        "nom",
        "latitude",
        "longitude",
        "capacite",
    ]

    missing_columns = [
        col for col in required_columns
        if col not in df_station.columns
    ]

    if missing_columns:
        raise ValueError(f"Colonnes manquantes dans station : {missing_columns}")

    if df_station["id_station"].isnull().any():
        raise ValueError("Qualité KO : id_station contient des valeurs nulles.")

    if df_station["nom"].isnull().any():
        raise ValueError("Qualité KO : nom contient des valeurs nulles.")

    if (df_station["nom"].astype(str).str.strip() == "").any():
        raise ValueError("Qualité KO : certaines stations ont un nom vide.")

    if (df_station["capacite"] < 0).any():
        raise ValueError("Qualité KO : certaines capacités sont négatives.")

    if df_station["id_station"].duplicated().any():
        raise ValueError("Qualité KO : doublons détectés sur id_station dans station.")

    print("Tests qualité station : OK")


def check_releve_quality(df_releve):
    """
    Vérifie la qualité des données de la table releve_disponibilite.
    """

    required_columns = [
        "id_station",
        "code_station",
        "date_collecte",
        "velos_disponibles",
        "bornes_disponibles",
        "velos_mecaniques",
        "velos_electriques",
        "est_installee",
        "retour_possible",
        "location_possible",
        "dernier_signalement",
    ]

    missing_columns = [
        col for col in required_columns
        if col not in df_releve.columns
    ]

    if missing_columns:
        raise ValueError(f"Colonnes manquantes dans releve_disponibilite : {missing_columns}")

    if df_releve["id_station"].isnull().any():
        raise ValueError("Qualité KO : id_station contient des valeurs nulles dans les relevés.")

    if df_releve["date_collecte"].isnull().any():
        raise ValueError("Qualité KO : date_collecte contient des valeurs nulles.")

    numeric_columns = [
        "velos_disponibles",
        "bornes_disponibles",
        "velos_mecaniques",
        "velos_electriques",
    ]

    for column in numeric_columns:
        if (df_releve[column] < 0).any():
            raise ValueError(f"Qualité KO : valeurs négatives détectées dans {column}.")

    if df_releve.duplicated(subset=["id_station", "date_collecte"]).any():
        raise ValueError(
            "Qualité KO : doublons détectés sur id_station + date_collecte."
        )

    print("Tests qualité relevé disponibilité : OK")


def check_relation_station_releve(df_station, df_releve):
    """
    Vérifie que chaque relevé de disponibilité correspond à une station existante.
    """

    station_ids = set(df_station["id_station"])
    releve_ids = set(df_releve["id_station"])

    missing_station_ids = releve_ids - station_ids

    if missing_station_ids:
        raise ValueError(
            f"Qualité KO : certains relevés n'ont pas de station associée : "
            f"{list(missing_station_ids)[:10]}"
        )

    print("Test relation station / relevé : OK")
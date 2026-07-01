# Procédure de Sauvegarde et Restauration — PostgreSQL

> **Plateforme :** Big Data Vélib  
> **Composant :** PostgreSQL (conteneur `velib_postgres`)  
> **Version :** 1.0 — 2026-07-01

---

## Table des matières

1. [Objectif](#1-objectif)
2. [Périmètre des données sauvegardées](#2-périmètre-des-données-sauvegardées)
3. [Procédure de sauvegarde](#3-procédure-de-sauvegarde)
4. [Vérification du backup](#4-vérification-du-backup)
5. [Procédure de restauration](#5-procédure-de-restauration)
6. [Vérification après restauration](#6-vérification-après-restauration)
7. [Politique de sauvegarde](#7-politique-de-sauvegarde)

---

## 1. Objectif

Ce document décrit la procédure de sauvegarde et de restauration des données PostgreSQL de la plateforme Big Data Vélib. Il couvre la création des sauvegardes, leur vérification et leur restauration en environnement de test.

PostgreSQL est utilisé pour deux usages principaux :

| Schéma | Rôle |
|---|---|
| `gold` | Tables analytiques de restitution consommées par Grafana |
| `monitoring` | Historique des contrôles de supervision qualité |

---

## 2. Périmètre des données sauvegardées

| Schéma | Contenu | Source |
|---|---|---|
| `gold` | Tables issues de la transformation Spark Gold | DAG `velib_gold_dag` |
| `monitoring` | Résultats des contrôles techniques et qualité | DAG `velib_monitoring` |

Les autres schémas (bronze, silver) sont exclus car ils sont recalculables depuis les sources HDFS.

---

## 3. Procédure de sauvegarde

### 3.1 Créer le répertoire de destination

```powershell
mkdir backups\postgres
```

### 3.2 Exécuter la sauvegarde

```powershell
$backupName = "velib_gold_monitoring_backup_$(Get-Date -Format 'yyyyMMdd_HHmmss').sql"

docker exec velib_postgres pg_dump `
    -U velib_user `
    -d velibdata `
    -n gold -n monitoring `
    -f /tmp/$backupName

docker cp velib_postgres:/tmp/$backupName backups\postgres\$backupName
```

Le fichier produit est nommé selon le format `velib_gold_monitoring_backup_YYYYMMDD_HHmmss.sql` et déposé dans :

```
backups/postgres/
```

---

## 4. Vérification du backup

### 4.1 Contrôle de présence

```powershell
dir backups\postgres
```

### 4.2 Contrôle de contenu

Vérifier que le dump contient bien des définitions de tables :

```powershell
Select-String -Path "backups\postgres\velib_gold_monitoring_backup_*.sql" -Pattern "CREATE TABLE"
```

Un résultat vide indique un dump vide ou corrompu — relancer la sauvegarde.

---

## 5. Procédure de restauration

> **Bonne pratique :** toujours restaurer dans une base de test isolée pour éviter d'écraser les données en production.

### 5.1 Créer la base de test

```powershell
docker exec -it velib_postgres psql -U velib_user -d postgres `
    -c "DROP DATABASE IF EXISTS velibdata_restore_test;"

docker exec -it velib_postgres psql -U velib_user -d postgres `
    -c "CREATE DATABASE velibdata_restore_test;"
```

### 5.2 Copier le fichier dans le conteneur

Remplacer `NOM_DU_BACKUP.sql` par le nom réel du fichier à restaurer :

```powershell
docker cp backups\postgres\NOM_DU_BACKUP.sql velib_postgres:/tmp/restore_test.sql
```

### 5.3 Lancer la restauration

```powershell
docker exec -it velib_postgres psql `
    -U velib_user `
    -d velibdata_restore_test `
    -f /tmp/restore_test.sql
```

---

## 6. Vérification après restauration

### 6.1 Lister les tables restaurées

```powershell
docker exec -it velib_postgres psql -U velib_user -d velibdata_restore_test -c "\dt gold.*"
docker exec -it velib_postgres psql -U velib_user -d velibdata_restore_test -c "\dt monitoring.*"
```

### 6.2 Contrôler les volumes de données

```powershell
docker exec -it velib_postgres psql -U velib_user -d velibdata_restore_test `
    -c "SELECT COUNT(*) FROM gold.disponibilite_courante;"

docker exec -it velib_postgres psql -U velib_user -d velibdata_restore_test `
    -c "SELECT COUNT(*) FROM monitoring.check_results;"
```

Un `COUNT` à zéro sur une table normalement alimentée signale un problème de restauration.

---

## 7. Politique de sauvegarde

| Paramètre | Valeur |
|---|---|
| Schémas couverts | `gold`, `monitoring` |
| Fréquence recommandée | Quotidienne |
| Rétention | 7 derniers fichiers |
| Emplacement | `backups/postgres/` |
| Vérification | Contrôle de présence + test de restauration ponctuel |
| Format | SQL plain-text (`pg_dump`) |

> Pour automatiser la rotation des backups (suppression des fichiers de plus de 7 jours), un script PowerShell planifié via le Planificateur de tâches Windows peut être mis en place.

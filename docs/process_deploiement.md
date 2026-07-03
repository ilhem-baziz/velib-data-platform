\# Processus d’automatisation du déploiement — Plateforme Big Data Vélib



\## 1. Objectif



Ce document décrit le processus d’automatisation du déploiement de la plateforme Big Data Vélib.



Il répond à la compétence liée à la conception d’un processus automatisé de déploiement avec les paramètres nécessaires et les outils adaptés.



\## 2. Outils utilisés



| Outil | Rôle |

|---|---|

| GitHub | Versionnement du code |

| GitHub Actions | Vérification automatique du projet |

| Docker Compose | Déploiement des services |

| PowerShell | Script local de déploiement |

| Airflow | Orchestration des traitements |

| Grafana | Supervision post-déploiement |



\## 3. Paramètres nécessaires



Les paramètres de déploiement sont centralisés dans le fichier `.env`.



Un modèle est fourni dans le fichier `.env.auto`.



Les principaux paramètres concernent :



\- PostgreSQL ;

\- MinIO ;

\- HDFS ;

\- Grafana SMTP ;

\- monitoring.



\## 4. Chaîne d’automatisation



Le processus suit les étapes suivantes :



```text

Push Git

→ GitHub Actions

→ Vérification docker compose

→ Vérification syntaxe Python

→ Vérification fichiers nécessaires

→ Déploiement local via script PowerShell

→ Démarrage Docker Compose

→ Vérification Airflow / Spark / Grafana


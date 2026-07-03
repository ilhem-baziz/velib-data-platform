Write-Host "=== Déploiement plateforme Big Data Vélib ===" -ForegroundColor Cyan

Write-Host "`n[1] Vérification de la configuration Docker Compose" -ForegroundColor Yellow
docker compose config

if ($LASTEXITCODE -ne 0) {
    Write-Host "Erreur : configuration docker-compose invalide." -ForegroundColor Red
    exit 1
}

Write-Host "`n[2] Construction des images nécessaires" -ForegroundColor Yellow
docker compose build

if ($LASTEXITCODE -ne 0) {
    Write-Host "Erreur : échec de build Docker." -ForegroundColor Red
    exit 1
}

Write-Host "`n[3] Démarrage des services" -ForegroundColor Yellow
docker compose up -d

if ($LASTEXITCODE -ne 0) {
    Write-Host "Erreur : échec du démarrage des services." -ForegroundColor Red
    exit 1
}

Write-Host "`n[4] Etat des services" -ForegroundColor Yellow
docker compose ps

Write-Host "`n[5] Vérification Spark" -ForegroundColor Yellow
docker exec velib_airflow_scheduler spark-submit --version

Write-Host "`n[6] Vérification Airflow" -ForegroundColor Yellow
docker exec velib_airflow_scheduler airflow dags list

Write-Host "`nDéploiement terminé." -ForegroundColor Green

import os

# URLs APIs Vélib
STATION_INFORMATION_URL = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json"
STATION_STATUS_URL = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json"



# MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "velib-data")

# PostgreSQL
POSTGRES_USER = os.getenv("POSTGRES_USER", "velib_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "velib_password")
POSTGRES_DB = os.getenv("POSTGRES_DB", "velibdata")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

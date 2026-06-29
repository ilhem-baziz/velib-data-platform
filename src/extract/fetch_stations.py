import io
import json
from datetime import datetime

import requests

from config import STATION_INFORMATION_URL, MINIO_BUCKET
from utils.minio_client import get_minio_client


def fetch_stations():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    response = requests.get(STATION_INFORMATION_URL, timeout=20)
    response.raise_for_status()

    data = response.json()

    content = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    object_name = f"bronze/station_information/station_information_{timestamp}.json"

    client = get_minio_client()
    client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        data=io.BytesIO(content),
        length=len(content),
        content_type="application/json",
    )

    print(f"Données stations envoyées dans MinIO : {object_name}")


if __name__ == "__main__":
    fetch_stations()
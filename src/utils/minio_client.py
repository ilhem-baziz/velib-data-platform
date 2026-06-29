from minio import Minio

from config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_BUCKET,
)


def get_minio_client():
    """
    Crée un client MinIO pour se connecter au stockage objet.
    """
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def upload_file_to_minio(local_path, object_name):
    """
    Envoie un fichier local vers MinIO.

    local_path : chemin du fichier sur le disque local
    object_name : chemin du fichier dans MinIO
    """
    client = get_minio_client()

    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)

    client.fput_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        file_path=str(local_path),
        content_type="application/json",
    )
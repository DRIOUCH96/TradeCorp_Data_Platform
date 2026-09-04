import os
import shutil
import tempfile
from pathlib import Path
from typing import Dict

from azure.storage.blob import BlobServiceClient
from pyspark.sql import DataFrame


def _create_blob_service_client() -> BlobServiceClient:
    account_name = os.environ["AZURE_STORAGE_ACCOUNT_NAME"]
    account_url = (
        account_name
        if account_name.startswith("http")
        else f"https://{account_name}.blob.core.windows.net"
    )
    return BlobServiceClient(
        account_url=account_url,
        credential=os.environ["AZURE_STORAGE_ACCOUNT_KEY"],
    )


def _blob_prefix_from_path(path: str) -> str:
    """Transforme un chemin local ou blob en préfixe de fichier blob."""

    normalized_path = path.replace("\\", "/").rstrip("/")
    if normalized_path.startswith("/home/jovyan/"):
        return Path(normalized_path).name
    return normalized_path.lstrip("/")


def write_parquet(
    df: DataFrame,
    path: str,
    container_name: str | None = None,
) -> None:
    """Écrit un DataFrame en Parquet dans le conteneur Azure clean."""

    container_name = container_name or os.getenv(
        "AZURE_CLEAN_CONTAINER",
        "clean",
    )
    blob_prefix = _blob_prefix_from_path(path)
    temporary_directory = tempfile.mkdtemp(prefix="tradecorp_parquet_")

    try:
        df.write.mode("overwrite").parquet(temporary_directory)
        container_client = _create_blob_service_client().get_container_client(
            container_name
        )

        for local_file in Path(temporary_directory).rglob("*"):
            if not local_file.is_file() or local_file.name.endswith(".crc"):
                continue
            relative_path = local_file.relative_to(temporary_directory).as_posix()
            blob_name = f"{blob_prefix}/{relative_path}"
            with local_file.open("rb") as data:
                container_client.upload_blob(
                    name=blob_name,
                    data=data,
                    overwrite=True,
                )
            print(f"Parquet téléversé : {blob_name}")
    finally:
        shutil.rmtree(temporary_directory, ignore_errors=True)


def write_postgres(
    df: DataFrame,
    table_name: str,
    jdbc_url: str,
    properties: Dict[str, str],
) -> None:
    """Écrit un DataFrame dans une table PostgreSQL."""

    df.write.jdbc(
        url=jdbc_url,
        table=table_name,
        mode="overwrite",
        properties=properties,
    )
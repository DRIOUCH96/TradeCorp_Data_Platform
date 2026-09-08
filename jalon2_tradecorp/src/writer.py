import logging
import os
import tempfile
from pathlib import Path

from pyspark.sql import DataFrame,SparkSession

from utils import create_blob_service_client

LOGGER = logging.getLogger(__name__)

def delete_existing_output(
    container_client,
    blob_prefix: str,
) -> int:
    """Supprime le résultat précédent dans ADLS."""

    prefix = f"{blob_prefix.rstrip('/')}/"

    existing_blobs = list(
        container_client.list_blobs(
            name_starts_with=prefix
        )
    )

    for blob in existing_blobs:
        container_client.delete_blob(blob.name)

    return len(existing_blobs)
def write_parquet(
    df: DataFrame,
    output_name: str = "orders_enriched.parquet",
) -> str:
    """Écrit le DataFrame en Parquet puis l'envoie dans ADLS clean."""

    container_name = os.getenv(
        "AZURE_CLEAN_CONTAINER",
        "clean",
    )
    blob_prefix = output_name.replace("\\", "/").strip("/")

    with tempfile.TemporaryDirectory(
        prefix="tradecorp_parquet_"
    ) as temporary_directory:
        parquet_directory = (
            Path(temporary_directory) / "parquet"
        )

        LOGGER.info(
            "Écriture locale du Parquet dans %s",
            parquet_directory,
        )
        (
        df.coalesce(1)
            .write
            .mode("overwrite")
            .parquet(str(parquet_directory))
        )

        container_client = (
            create_blob_service_client()
            .get_container_client(container_name)
        )
        deleted_count = delete_existing_output(
            container_client,
            blob_prefix,
        )

        LOGGER.info(
            "%s ancien(s) blob(s) supprimé(s) "
            "dans %s/%s",
            deleted_count,
            container_name,
            blob_prefix,
        )

        for local_file in parquet_directory.rglob("*"):
            if not local_file.is_file():
                continue

            if local_file.name.endswith(".crc"):
                continue

            relative_path = local_file.relative_to(
                parquet_directory
            ).as_posix()

            blob_name = f"{blob_prefix}/{relative_path}"

            with local_file.open("rb") as file_handle:
                container_client.upload_blob(
                    name=blob_name,
                    data=file_handle,
                    overwrite=True,
                )

            LOGGER.info(
                "Fichier Parquet envoyé : %s/%s",
                container_name,
                blob_name,
            )

    return f"{container_name}/{blob_prefix}"
def main() -> None:
    """Publie le Parquet intermédiaire dans ADLS clean."""

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | %(levelname)s | "
            "%(name)s | %(message)s"
        ),
    )

    spark = None

    try:
        staging_path = os.getenv(
            "STAGING_PARQUET_PATH",
            (
                "/home/jovyan/data/staging/"
                "orders_enriched"
            ),
        )

        if not Path(staging_path).exists():
            raise FileNotFoundError(
                f"Parquet intermédiaire absent : {staging_path}"
            )

        spark = (
            SparkSession.builder
            .appName("TradeCorp Writer")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        dataframe = spark.read.parquet(staging_path)

        LOGGER.info(
            "Parquet intermédiaire chargé : %s lignes",
            dataframe.count(),
        )

        destination = write_parquet(
            dataframe,
            os.getenv(
                "CLEAN_OUTPUT_PATH",
                "orders_enriched",
            ),
        )

        LOGGER.info(
            "Upload ADLS terminé avec succès : %s",
            destination,
        )

    except Exception:
        LOGGER.exception("Échec de l’écriture ADLS")
        raise

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
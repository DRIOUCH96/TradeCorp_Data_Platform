import os
import tempfile
from pathlib import Path

from azure.storage.blob import BlobServiceClient
from pyspark.sql import DataFrame, SparkSession


BUSINESS_CSV_FILES = (
    "categories.csv",
    "customers.csv",
    "employees.csv",
    "order_details.csv",
    "orders.csv",
    "products.csv",
    "shippers.csv",
    "suppliers.csv",
)


def create_blob_service_client() -> BlobServiceClient:
    """Construit le client Azure depuis les variables d'environnement."""

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


def download_business_csvs(
    destination: str,
    container_name: str | None = None,
) -> dict[str, str]:
    """Télécharge uniquement les huit CSV métier dans un dossier local."""

    destination_path = Path(destination)
    destination_path.mkdir(parents=True, exist_ok=True)
    container_name = container_name or os.getenv("AZURE_RAW_CONTAINER", "raw")
    container_client = create_blob_service_client().get_container_client(
        container_name
    )

    local_paths = {}
    for filename in BUSINESS_CSV_FILES:
        local_path = destination_path / filename
        blob_client = container_client.get_blob_client(filename)
        with local_path.open("wb") as file_handle:
            file_handle.write(blob_client.download_blob().readall())
        local_paths[filename.removesuffix(".csv")] = str(local_path)
        print(f"Téléchargé : {filename}")

    return local_paths


def read_csv(
    spark: SparkSession,
    path: str,
    filename: str,
) -> DataFrame:
    """Lit un fichier CSV avec son en-tête et infère son schéma."""

    full_path = f"{path.rstrip('/')}/{filename}"
    return spark.read.csv(
        full_path,
        header=True,
        inferSchema=True,
    )


def read_business_csvs(
    spark: SparkSession,
    destination: str,
) -> dict[str, DataFrame]:
    """Télécharge puis lit les huit CSV métier avec Spark."""

    local_paths = download_business_csvs(destination)
    return {
        name: spark.read.csv(
            path,
            header=True,
            inferSchema=True,
        )
        for name, path in local_paths.items()
    }


def read_country_currency_reference(
    spark: SparkSession,
    path: str = "/home/jovyan/data/raw/reference/country_currency.csv",
) -> DataFrame:
    """Lit la référence pays-devise utilisée par l'enrichissement."""

    return spark.read.csv(path, header=True, inferSchema=True)


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("TradeCorp CSV Reader")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        temporary_directory = tempfile.mkdtemp(
            prefix="tradecorp_raw_",
            dir=os.getenv("LOCAL_TMP_DIR", "/home/jovyan/data/tmp"),
        )
        dataframes = read_business_csvs(spark, temporary_directory)
        for name, dataframe in dataframes.items():
            print(f"{name}: {dataframe.count()} ligne(s)")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

import os
import tempfile

from pyspark.sql import SparkSession

from fetch_exchange_rates import fetch_exchange_rates
from reader import read_business_csvs, read_country_currency_reference
from enrichment import build_enriched
from writer import write_parquet


def main() -> None:
    """Orchestre la lecture Azure, la transformation et l'écriture Parquet."""

    spark = (
        SparkSession.builder
        .appName("TradeCorp Data Pipeline")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    temporary_directory = tempfile.mkdtemp(
        prefix="tradecorp_raw_",
        dir=os.getenv("LOCAL_TMP_DIR", "/home/jovyan/data/tmp"),
    )
    output_path = os.getenv(
        "CLEAN_OUTPUT_PATH",
        "orders_enriched.parquet",
    )

    try:
        print("1/3 - Lecture des CSV depuis le conteneur raw")
        raw_dataframes = read_business_csvs(
            spark,
            temporary_directory,
        )
        country_currency = read_country_currency_reference(spark)
        base_currency = os.getenv("EXCHANGE_RATE_BASE", "EUR")
        exchange_rates = fetch_exchange_rates(base_currency)

        print("2/3 - Transformation et enrichissement")
        enriched_dataframe = build_enriched(
            raw_dataframes,
            country_currency=country_currency,
            exchange_rates=exchange_rates,
        )
        row_count = enriched_dataframe.count()
        print(f"Lignes enrichies : {row_count}")

        print("3/3 - Écriture Parquet dans le conteneur clean")
        write_parquet(enriched_dataframe, output_path)
        print(f"Pipeline terminé : {output_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
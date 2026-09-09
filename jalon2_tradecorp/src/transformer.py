import logging
import os
import tempfile
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from enrichment import add_currency_column
from reader import read_business_csvs, read_reference_files

from utils import (
    add_sous_total,
    clean_customers,
    clean_employees,
    clean_order_details,
    clean_orders,
    clean_products,
)


FINAL_COLUMNS = [
    "order_id",
    "customer_id",
    "employee_id",
    "product_id",
    "order_date",
    "required_date",
    "shipped_date",
    "freight",
    "is_shipped",
    "prix_unitaire",
    "quantite",
    "discount",
    "sous_total",
    "customer_name",
    "customer_country",
    "customer_city",
    "product_name",
    "category_name",
    "en_stock",
    "full_name",
    "shipper_name",
]
LOGGER = logging.getLogger(__name__)

def build_enriched(
    dataframes: dict[str, DataFrame],
) -> DataFrame:
    """Nettoie et joint les sept tables du DataFrame final."""

    customers = clean_customers(
        dataframes["customers"]
    ).select(
        "customer_id",
        F.col("company_name").alias("customer_name"),
        F.col("country").alias("customer_country"),
        F.col("city").alias("customer_city"),
    )

    orders = clean_orders(
        dataframes["orders"]
    ).select(
        "order_id",
        "customer_id",
        "employee_id",
        "shipper_id",
        "order_date",
        "required_date",
        "shipped_date",
        "freight",
        "is_shipped",
    )

    order_details = add_sous_total(
        clean_order_details(dataframes["order_details"])
    ).select(
        "order_id",
        "product_id",
        "prix_unitaire",
        "quantite",
        "discount",
        "sous_total",
    )

    products = clean_products(
        dataframes["products"]
    ).select(
        "product_id",
        "category_id",
        "product_name",
        "en_stock",
    )

    categories = dataframes["categories"].select(
        "category_id",
        "category_name",
    )

    employees = clean_employees(
        dataframes["employees"]
    ).select(
        "employee_id",
        "full_name",
    )

    shippers = dataframes["shippers"].select(
        "shipper_id",
        F.col("company_name").alias("shipper_name"),
    )

    enriched = (
        order_details
        .join(orders, on="order_id", how="inner")
        .join(customers, on="customer_id", how="left")
        .join(products, on="product_id", how="left")
        .join(categories, on="category_id", how="left")
        .join(employees, on="employee_id", how="left")
        .join(shippers, on="shipper_id", how="left")
    )

    return enriched.select(*FINAL_COLUMNS)
def main() -> None:
    """Produit le Parquet intermédiaire enrichi."""

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | %(levelname)s | "
            "%(name)s | %(message)s"
        ),
    )

    spark = None

    try:
        spark = (
            SparkSession.builder
            .appName("TradeCorp Transformer")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        temporary_root = Path(
            os.getenv(
                "LOCAL_TMP_DIR",
                "/home/jovyan/data/tmp",
            )
        )
        temporary_root.mkdir(parents=True, exist_ok=True)

        with tempfile.TemporaryDirectory(
            prefix="tradecorp_transformer_",
            dir=temporary_root,
        ) as temporary_directory:
            temporary_path = Path(temporary_directory)

            dataframes = read_business_csvs(
                spark,
                temporary_path / "business",
            )

            transformed = build_enriched(dataframes)

            country_currency, exchange_rates = (
                read_reference_files(
                    spark,
                    temporary_path / "reference",
                )
            )

            enriched = add_currency_column(
                transformed,
                country_currency,
                exchange_rates,
            )

            staging_path = os.getenv(
                "STAGING_PARQUET_PATH",
                (
                    "/home/jovyan/data/staging/"
                    "orders_enriched"
                ),
            )

            row_count = enriched.count()

            (
                enriched.write
                .mode("overwrite")
                .parquet(staging_path)
            )

            LOGGER.info(
                "Transformation terminée : %s lignes",
                row_count,
            )
            LOGGER.info(
                "Parquet intermédiaire créé : %s",
                staging_path,
            )

    except Exception:
        LOGGER.exception("Échec de la transformation")
        raise

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils import (
    clean_costumers,
    clean_employees,
    clean_order_details as _clean_order_details,
    clean_orders as _clean_orders,
    clean_products,
)


def clean_customers(df: DataFrame) -> DataFrame:
    """Nettoie les clients via la fonction existante dans utils.py."""

    return clean_costumers(df)


def clean_orders(df: DataFrame) -> DataFrame:
    """Nettoie les commandes via la fonction existante dans utils.py."""

    if "freight" not in df.columns:
        df = df.withColumn("freight", F.lit(None).cast("double"))

    return _clean_orders(df)


def clean_order_details(df: DataFrame) -> DataFrame:
    """Nettoie les détails de commande via utils.py."""

    return _clean_order_details(df)


def add_sous_total(df: DataFrame) -> DataFrame:
    """Calcule le sous-total d'une ligne de commande."""

    return df.withColumn(
        "sous_total",
        F.round(
            F.col("prix_unitaire")
            * F.col("quantite")
            * (F.lit(1.0) - F.col("discount")),
            2,
        ),
    )


def add_local_currency(
    df: DataFrame,
    country_currency: DataFrame,
    exchange_rates: dict[str, float],
    base_currency: str = "EUR",
) -> DataFrame:
    """Ajoute la devise client et convertit le sous-total au taux du jour."""

    reference = country_currency.select(
        F.upper(F.trim(F.col("country"))).alias("customer_country"),
        F.upper(F.trim(F.col("currency"))).alias("currency"),
    )
    rates = df.sparkSession.createDataFrame(
        [
            (currency.upper(), float(rate))
            for currency, rate in exchange_rates.items()
        ],
        ["currency", "exchange_rate"],
    ).withColumnRenamed("currency", "rate_currency")

    result = df.join(reference, on="customer_country", how="left")
    result = result.join(
        rates,
        result["currency"] == rates["rate_currency"],
        how="left",
    ).drop("rate_currency")
    return result.withColumn(
        "sous_total_local",
        F.round(F.col("sous_total") * F.col("exchange_rate"), 2),
    ).drop("exchange_rate")


def _prefix_non_keys(df: DataFrame, prefix: str, keys: set[str]) -> DataFrame:
    """Préfixe les colonnes non utilisées comme clés de jointure."""

    return df.select(
        *[
            F.col(column_name).alias(
                column_name
                if column_name in keys
                else f"{prefix}_{column_name}"
            )
            for column_name in df.columns
        ]
    )


__all__ = [
    "add_local_currency",
    "add_sous_total",
    "clean_customers",
    "clean_order_details",
    "clean_orders",
]
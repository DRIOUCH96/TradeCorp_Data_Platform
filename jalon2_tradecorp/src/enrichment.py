from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from transformer import (
    add_sous_total,
    clean_customers,
    clean_employees,
    clean_order_details,
    clean_orders,
    clean_products,
)


def add_local_currency(
    df: DataFrame,
    country_currency: DataFrame,
    exchange_rates: dict[str, float],
) -> DataFrame:
    """Ajoute la devise du client et convertit son sous-total au taux du jour."""

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


def _prefix_non_keys(
    df: DataFrame,
    prefix: str,
    keys: set[str],
) -> DataFrame:
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


def build_enriched(
    dataframes: dict[str, DataFrame],
    country_currency: DataFrame,
    exchange_rates: dict[str, float],
) -> DataFrame:
    """Construit les commandes enrichies avec la devise du client."""

    customers = clean_customers(dataframes["customers"])
    orders = clean_orders(dataframes["orders"])
    order_details = add_sous_total(
        clean_order_details(dataframes["order_details"])
    )
    employees = clean_employees(dataframes["employees"])
    products = clean_products(dataframes["products"])

    enriched = orders
    for dataframe, prefix, keys, join_key in [
        (customers, "customer", {"customer_id"}, "customer_id"),
        (employees, "employee", {"employee_id"}, "employee_id"),
        (
            order_details,
            "order_detail",
            {"order_id", "product_id"},
            "order_id",
        ),
        (
            products,
            "product",
            {"product_id", "supplier_id", "category_id"},
            "product_id",
        ),
        (dataframes["categories"], "category", {"category_id"}, "category_id"),
        (dataframes["suppliers"], "supplier", {"supplier_id"}, "supplier_id"),
        (dataframes["shippers"], "shipper", {"shipper_id"}, "shipper_id"),
    ]:
        enriched = enriched.join(
            _prefix_non_keys(dataframe, prefix, keys),
            on=join_key,
            how="left",
        )

    return add_local_currency(enriched, country_currency, exchange_rates)


__all__ = ["add_local_currency", "build_enriched"]

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_currency_column(
    df: DataFrame,
    country_currency: DataFrame,
    exchange_rates: dict[str, float],
) -> DataFrame:
    """Ajoute la devise du client et convertit le sous-total."""

    if not exchange_rates:
        raise ValueError("Aucun taux de change n'a été fourni")

    # Mémorise l'ordre des colonnes produit par transformer.py.
    source_columns = df.columns

    reference = (
        country_currency.select(
            F.upper(
                F.trim(F.col("country"))
            ).alias("customer_country"),
            F.upper(
                F.trim(F.col("currency"))
            ).alias("currency"),
        )
        .dropDuplicates(["customer_country"])
    )

    normalized_rates = {
        currency.upper(): float(rate)
        for currency, rate in exchange_rates.items()
    }

    rate_mapping = F.create_map(
        *[
            item
            for currency, rate in normalized_rates.items()
            for item in (
                F.lit(currency),
                F.lit(rate),
            )
        ]
    )

    enriched = df.join(
        reference,
        on="customer_country",
        how="left",
    )

    return enriched.withColumn(
        "sous_total_local",
        F.round(
            F.col("sous_total")
            * F.element_at(
                rate_mapping,
                F.col("currency"),
            ),
            2,
        ),
    ).select(
        *source_columns,
        "currency",
        "sous_total_local",
    )
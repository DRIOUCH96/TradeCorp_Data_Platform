from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
)


def clean_customers(df: DataFrame) -> DataFrame:
    """Nettoie les chaînes, le nom du contact et le pays."""

    result = df

    string_columns = [
        field.name
        for field in result.schema.fields
        if isinstance(field.dataType, StringType)
    ]

    for column_name in string_columns:
        result = result.withColumn(
            column_name,
            F.trim(F.col(column_name)),
        )

    result = (
        result
        .withColumn(
            "contact_name",
            F.initcap(F.col("contact_name")),
        )
        .withColumn(
            "country",
            F.upper(F.col("country")),
        )
        .dropDuplicates(["customer_id"])
    )

    return result


def clean_orders(df: DataFrame) -> DataFrame:
    """Supprime les commandes non livrées et corrige les types."""

    result = df.dropna(
        subset=["shipped_date"]
    )

    for column_name in [
        "order_date",
        "required_date",
        "shipped_date",
    ]:
        result = result.withColumn(
            column_name,
            F.to_date(
                F.col(column_name),
                "yyyy-MM-dd",
            ),
        )

    if "ship_via" in result.columns:
        result = result.withColumnRenamed(
            "ship_via",
            "shipper_id",
        )

    result = result.withColumn(
        "is_shipped",
        F.col("shipped_date").isNotNull(),
    )

    return result


def clean_order_details(df: DataFrame) -> DataFrame:
    """Corrige les types et renomme les colonnes."""

    result = (
        df
        .withColumn(
            "unit_price",
            F.col("unit_price").cast(DoubleType()),
        )
        .withColumn(
            "quantity",
            F.col("quantity").cast(IntegerType()),
        )
        .withColumn(
            "discount",
            F.col("discount").cast(DoubleType()),
        )
        .withColumnRenamed(
            "unit_price",
            "prix_unitaire",
        )
        .withColumnRenamed(
            "quantity",
            "quantite",
        )
    )

    return result


def add_sous_total(df: DataFrame) -> DataFrame:
    """Ajoute le montant après application de la remise."""

    return df.withColumn(
        "sous_total",
        F.round(
            F.col("prix_unitaire")
            * F.col("quantite")
            * (
                F.lit(1.0)
                - F.col("discount")
            ),
            2,
        ),
    )
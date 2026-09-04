import sys

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

sys.path.insert(
    0,
    "/home/jovyan/src",
)

from transformer import (
    add_local_currency,
    add_sous_total,
    clean_customers,
    clean_orders,
)


@pytest.fixture(scope="session")
def spark():
    """Crée une SparkSession commune à tous les tests."""

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("TradeCorp Tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


def test_clean_orders_supprime_shipped_date_null(
    spark,
):
    """Vérifie la suppression des commandes non livrées."""

    schema = StructType(
        [
            StructField(
                "order_id",
                IntegerType(),
                False,
            ),
            StructField(
                "order_date",
                StringType(),
                True,
            ),
            StructField(
                "required_date",
                StringType(),
                True,
            ),
            StructField(
                "shipped_date",
                StringType(),
                True,
            ),
            StructField(
                "ship_via",
                IntegerType(),
                True,
            ),
        ]
    )

    data = [
        (
            1,
            "1997-01-01",
            "1997-01-10",
            "1997-01-05",
            1,
        ),
        (
            2,
            "1997-02-01",
            "1997-02-10",
            None,
            2,
        ),
    ]

    df = spark.createDataFrame(
        data,
        schema,
    )

    result = clean_orders(df)

    assert result.count() == 1

    assert (
        result
        .filter("shipped_date IS NULL")
        .count()
        == 0
    )

    assert "shipper_id" in result.columns
    assert "ship_via" not in result.columns
    assert result.first()["order_id"] == 1
    assert result.first()["is_shipped"] is True


def test_add_sous_total_calcule_correctement(
    spark,
):
    """Vérifie le calcul du sous-total avec remise."""

    schema = StructType(
        [
            StructField(
                "prix_unitaire",
                DoubleType(),
                False,
            ),
            StructField(
                "quantite",
                IntegerType(),
                False,
            ),
            StructField(
                "discount",
                DoubleType(),
                False,
            ),
        ]
    )

    data = [
        (
            10.0,
            2,
            0.1,
        )
    ]

    df = spark.createDataFrame(
        data,
        schema,
    )

    result = add_sous_total(df)

    sous_total = result.first()[
        "sous_total"
    ]

    assert sous_total == pytest.approx(
        18.0
    )


def test_clean_customers_applique_trim_et_initcap(
    spark,
):
    """Vérifie le nettoyage des données clients."""

    schema = StructType(
        [
            StructField(
                "customer_id",
                StringType(),
                False,
            ),
            StructField(
                "company_name",
                StringType(),
                True,
            ),
            StructField(
                "contact_name",
                StringType(),
                True,
            ),
            StructField(
                "country",
                StringType(),
                True,
            ),
        ]
    )

    data = [
        (
            "TEST1",
            "  entreprise test  ",
            "  jean dupont  ",
            "  france  ",
        )
    ]

    df = spark.createDataFrame(
        data,
        schema,
    )

    result = clean_customers(df)

    customer = result.first()

    assert (
        customer["company_name"]
        == "entreprise test"
    )

    assert (
        customer["contact_name"]
        == "Jean Dupont"
    )

    assert customer["country"] == "FRANCE"


def test_add_local_currency_joint_devise_client_et_convertit(
    spark,
):
    amounts = spark.createDataFrame(
        [("FRANCE", 18.0), ("UK", 10.0)],
        ["customer_country", "sous_total"],
    )
    reference = spark.createDataFrame(
        [("FRANCE", "EUR"), ("UK", "GBP")],
        ["country", "currency"],
    )

    result = add_local_currency(
        amounts,
        reference,
        {"EUR": 1.0, "GBP": 0.86},
    ).orderBy("customer_country").collect()

    assert result[0]["currency"] == "EUR"
    assert result[0]["sous_total_local"] == pytest.approx(18.0)
    assert result[1]["currency"] == "GBP"
    assert result[1]["sous_total_local"] == pytest.approx(8.6)
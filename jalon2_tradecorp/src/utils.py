from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType
from azure.storage.blob import BlobServiceClient
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

def clean_customers(df: DataFrame) -> DataFrame:
    """Nettoie les champs texte des clients et supprime les doublons."""

    result = df
    for field in result.schema.fields:
        if isinstance(field.dataType, StringType):
            result = result.withColumn(
                field.name,
                F.trim(F.col(field.name)),
            )

    if "contact_name" in result.columns:
        result = result.withColumn(
            "contact_name",
            F.initcap(F.col("contact_name")),
        )
    if "country" in result.columns:
        result = result.withColumn(
            "country",
            F.upper(F.col("country")),
        )
    if "customer_id" in result.columns:
        result = result.dropDuplicates(["customer_id"])

    return result


def clean_costumers(df: DataFrame) -> DataFrame:
    """Alias historique de clean_customers, conservé pour compatibilité."""

    return clean_customers(df)


def clean_orders(df: DataFrame) -> DataFrame:
    """Supprime les commandes non livrées et convertit leurs types."""

    result = df
    if "shipped_date" in result.columns:
        result = result.dropna(subset=["shipped_date"])

    for column_name in ["order_date", "shipped_date", "required_date"]:
        if column_name in result.columns:
            result = result.withColumn(
                column_name,
                F.to_date(F.col(column_name), "yyyy-MM-dd"),
            )

    if "freight" in result.columns:
        result = result.withColumn(
            "freight",
            F.col("freight").cast(DoubleType()),
        )
    if "order_id" in result.columns:
        result = result.withColumn(
            "order_id",
            F.col("order_id").cast(IntegerType()),
        )
    if "ship_via" in result.columns:
        result = result.withColumnRenamed("ship_via", "shipper_id")
    if "shipped_date" in result.columns:
        result = result.withColumn(
            "is_shipped",
            F.col("shipped_date").isNotNull(),
        )

    return result


def clean_order_details(df: DataFrame) -> DataFrame:
    """Convertit les types des détails et calcule leur sous-total."""

    result = df
    for column_name, data_type in [
        ("unit_price", DoubleType()),
        ("quantity", IntegerType()),
        ("discount", DoubleType()),
    ]:
        if column_name in result.columns:
            result = result.withColumn(
                column_name,
                F.col(column_name).cast(data_type),
            )

    if "unit_price" in result.columns:
        result = result.withColumnRenamed("unit_price", "prix_unitaire")
    if "quantity" in result.columns:
        result = result.withColumnRenamed("quantity", "quantite")
    if {"prix_unitaire", "quantite", "discount"}.issubset(result.columns):
        result = result.withColumn(
            "sous_total",
            F.round(
                F.col("prix_unitaire")
                * F.col("quantite")
                * (F.lit(1.0) - F.col("discount")),
                2,
            ),
        )

    return result


def clean_employees(df: DataFrame) -> DataFrame:
    """Sélectionne les champs utiles des employés et convertit les dates."""

    selected_columns = [
        "employee_id",
        "last_name",
        "first_name",
        "title",
        "hire_date",
        "city",
        "region",
        "postal_code",
        "country",
    ]
    result = df.select(
        *[column_name for column_name in selected_columns if column_name in df.columns]
    )
    if "hire_date" in result.columns:
        result = result.withColumn(
            "hire_date",
            F.to_date(F.col("hire_date"), "yyyy-MM-dd"),
        )
    if {"first_name", "last_name"}.issubset(result.columns):
        result = result.withColumn(
            "full_name",
            F.concat_ws(" ", "first_name", "last_name"),
        )
    return result


def clean_products(df: DataFrame) -> DataFrame:
    """Convertit les types produits et ajoute l'indicateur de stock."""

    result = df
    for column_name, data_type in [
        ("unit_price", DoubleType()),
        ("units_in_stock", IntegerType()),
        ("units_on_order", IntegerType()),
        ("reorder_level", IntegerType()),
    ]:
        if column_name in result.columns:
            result = result.withColumn(
                column_name,
                F.col(column_name).cast(data_type),
            )
    if "units_in_stock" in result.columns:
        result = result.withColumn(
            "en_stock",
            F.col("units_in_stock") > 0,
        )
    return result


def _prefix_non_keys(df: DataFrame, prefix: str, keys: set[str]) -> DataFrame:
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


def build_enriched(dataframes: dict[str, DataFrame]) -> DataFrame:
    """Nettoie les sources et les joint en un DataFrame enrichi."""

    enriched = clean_orders(dataframes["orders"])
    sources = [
        (clean_customers(dataframes["customers"]), "customer", {"customer_id"}, "customer_id"),
        (clean_employees(dataframes["employees"]), "employee", {"employee_id"}, "employee_id"),
        (clean_order_details(dataframes["order_details"]), "order_detail", {"order_id", "product_id"}, "order_id"),
        (clean_products(dataframes["products"]), "product", {"product_id", "supplier_id", "category_id"}, "product_id"),
        (dataframes["categories"], "category", {"category_id"}, "category_id"),
        (dataframes["suppliers"], "supplier", {"supplier_id"}, "supplier_id"),
        (dataframes["shippers"], "shipper", {"shipper_id"}, "shipper_id"),
    ]
    for dataframe, prefix, keys, join_key in sources:
        enriched = enriched.join(
            _prefix_non_keys(dataframe, prefix, keys),
            on=join_key,
            how="left",
        )
    return enriched
def build_enriched(
    dataframes: dict[str, DataFrame],
    country_currency: DataFrame | None = None,
    exchange_rates: dict[str, float] | None = None,
    base_currency: str = "EUR",
) -> DataFrame:
    """Nettoie les sources et construit le DataFrame enrichi final."""

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

    if country_currency is not None and exchange_rates is not None:
        enriched = add_local_currency(
            enriched,
            country_currency,
            exchange_rates,
            base_currency,
        )

    return enriched

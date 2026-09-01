from collections import Counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
)

from reader import read_csv
from transformer import (
    add_sous_total,
    clean_customers,
    clean_order_details,
    clean_orders,
)
from writer import (
    write_parquet,
    write_postgres,
)


DATA_PATH = "/home/jovyan/data"

OUTPUT_PATH = (
    "/home/jovyan/data/output/"
    "orders_enriched.parquet"
)

JDBC_URL = (
    "jdbc:postgresql://postgres:5432/tradecorp"
)

POSTGRES_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
}


def rename_columns(df, correspondences):
    """Renomme plusieurs colonnes d'un DataFrame."""

    result = df

    for old_name, new_name in correspondences.items():
        if old_name in result.columns:
            result = result.withColumnRenamed(
                old_name,
                new_name,
            )

    return result


def main():
    spark = (
        SparkSession.builder
        .appName("TradeCorp ETL Pipeline")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        print("1/7 - Lecture des huit fichiers CSV")

        customers_raw = read_csv(
            spark,
            DATA_PATH,
            "customers.csv",
        )

        orders_raw = read_csv(
            spark,
            DATA_PATH,
            "orders.csv",
        )

        order_details_raw = read_csv(
            spark,
            DATA_PATH,
            "order_details.csv",
        )

        products_raw = read_csv(
            spark,
            DATA_PATH,
            "products.csv",
        )

        categories = read_csv(
            spark,
            DATA_PATH,
            "categories.csv",
        )

        suppliers = read_csv(
            spark,
            DATA_PATH,
            "suppliers.csv",
        )

        employees_raw = read_csv(
            spark,
            DATA_PATH,
            "employees.csv",
        )

        shippers = read_csv(
            spark,
            DATA_PATH,
            "shippers.csv",
        )

        raw_dataframes = {
            "customers": customers_raw,
            "orders": orders_raw,
            "order_details": order_details_raw,
            "products": products_raw,
            "categories": categories,
            "suppliers": suppliers,
            "employees": employees_raw,
            "shippers": shippers,
        }

        for name, dataframe in raw_dataframes.items():
            print(
                f"  - {name}: "
                f"{dataframe.count()} ligne(s)"
            )

        print("2/7 - Nettoyage des données")

        customers = clean_customers(
            customers_raw
        )

        orders = clean_orders(
            orders_raw
        )

        order_details = add_sous_total(
            clean_order_details(
                order_details_raw
            )
        )

        median_values = (
            products_raw.approxQuantile(
                "unit_price",
                [0.5],
                0.01,
            )
        )

        median_unit_price = (
            median_values[0]
            if median_values
            else 0.0
        )

        products = (
            products_raw
            .withColumn(
                "unit_price",
                F.col("unit_price").cast(
                    DoubleType()
                ),
            )
            .withColumn(
                "units_in_stock",
                F.col("units_in_stock").cast(
                    IntegerType()
                ),
            )
            .withColumn(
                "discontinued",
                F.col("discontinued").cast(
                    IntegerType()
                ),
            )
            .fillna(
                {
                    "unit_price":
                        median_unit_price
                }
            )
            .withColumn(
                "en_stock",
                F.col("units_in_stock") > 0,
            )
        )

        employees = (
            employees_raw
            .select(
                "employee_id",
                "first_name",
                "last_name",
                "title",
                "hire_date",
                "city",
                "country",
            )
            .withColumn(
                "hire_date",
                F.to_date(
                    F.col("hire_date"),
                    "yyyy-MM-dd",
                ),
            )
            .withColumn(
                "full_name",
                F.concat_ws(
                    " ",
                    F.col("first_name"),
                    F.col("last_name"),
                ),
            )
        )

        print("3/7 - Renommage des colonnes")

        customers = rename_columns(
            customers,
            {
                "company_name":
                    "customer_company_name",
                "contact_name":
                    "customer_contact_name",
                "contact_title":
                    "customer_contact_title",
                "address":
                    "customer_address",
                "city":
                    "customer_city",
                "region":
                    "customer_region",
                "postal_code":
                    "customer_postal_code",
                "country":
                    "customer_country",
                "phone":
                    "customer_phone",
                "fax":
                    "customer_fax",
            },
        )

        products = rename_columns(
            products,
            {
                "unit_price":
                    "product_unit_price",
            },
        )

        categories = rename_columns(
            categories,
            {
                "description":
                    "category_description",
            },
        )

        employees = rename_columns(
            employees,
            {
                "first_name":
                    "employee_first_name",
                "last_name":
                    "employee_last_name",
                "title":
                    "employee_title",
                "hire_date":
                    "employee_hire_date",
                "city":
                    "employee_city",
                "country":
                    "employee_country",
                "full_name":
                    "employee_full_name",
            },
        )

        shippers = rename_columns(
            shippers,
            {
                "company_name":
                    "shipper_name",
                "phone":
                    "shipper_phone",
            },
        )

        print("4/7 - Enrichissement des produits")

        products_enriched = (
            products
            .join(
                categories,
                "category_id",
                "inner",
            )
        )

        print("5/7 - Construction du DataFrame enrichi")

        orders_enriched = (
            order_details
            .join(
                orders,
                "order_id",
                "inner",
            )
            .join(
                customers,
                "customer_id",
                "inner",
            )
            .join(
                products_enriched,
                "product_id",
                "inner",
            )
            .join(
                employees,
                "employee_id",
                "left",
            )
            .join(
                shippers,
                "shipper_id",
                "left",
            )
        )

        duplicate_columns = {
            column_name: number
            for column_name, number
            in Counter(
                orders_enriched.columns
            ).items()
            if number > 1
        }

        if duplicate_columns:
            raise ValueError(
                "Colonnes dupliquées : "
                f"{duplicate_columns}"
            )

        orders_enriched.cache()

        number_of_rows = (
            orders_enriched.count()
        )

        print(
            "Nombre de lignes enrichies :",
            number_of_rows,
        )

        print(
            "Nombre de colonnes :",
            len(orders_enriched.columns),
        )

        print("6/7 - Écriture en Parquet")

        write_parquet(
            orders_enriched,
            OUTPUT_PATH,
        )

        print(
            "Parquet créé :",
            OUTPUT_PATH,
        )

        print("7/7 - Écriture dans PostgreSQL")

        write_postgres(
            orders_enriched,
            "orders_enriched",
            JDBC_URL,
            POSTGRES_PROPERTIES,
        )

        print(
            "Table PostgreSQL créée : "
            "orders_enriched"
        )

        print(
            "Pipeline TradeCorp terminé "
            "avec succès !"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
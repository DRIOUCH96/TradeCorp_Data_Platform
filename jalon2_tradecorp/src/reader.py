from pyspark.sql import DataFrame, SparkSession


def read_csv(
    spark: SparkSession,
    path: str,
    filename: str,
) -> DataFrame:
    """Lit un fichier CSV avec son en-tête et infère son schéma."""

    full_path = f"{path.rstrip('/')}/{filename}"

    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(full_path)
    )
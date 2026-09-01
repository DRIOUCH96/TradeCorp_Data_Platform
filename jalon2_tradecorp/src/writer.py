from typing import Dict

from pyspark.sql import DataFrame


def write_parquet(
    df: DataFrame,
    path: str,
) -> None:
    """Écrit un DataFrame en Parquet."""

    (
        df.write
        .mode("overwrite")
        .parquet(path)
    )


def write_postgres(
    df: DataFrame,
    table_name: str,
    jdbc_url: str,
    properties: Dict[str, str],
) -> None:
    """Écrit un DataFrame dans une table PostgreSQL."""

    (
        df.write
        .jdbc(
            url=jdbc_url,
            table=table_name,
            mode="overwrite",
            properties=properties,
        )
    )
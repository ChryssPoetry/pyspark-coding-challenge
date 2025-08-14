"""
Utility functions for the pyspark‑coding‑challenge project.

This module contains helpers to obtain a Spark session, read CSVs with
a provided schema, and coerce JSON strings into the proper complex
types used by the impressions dataset.  Keeping these helpers
centralized simplifies configuration across scripts and tests.
"""

from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType

from .schema import impression_struct
from .constants import COL_IMPRESSIONS


def get_spark(app_name: str = "PySparkChallenge") -> SparkSession:
    """Create or retrieve a local SparkSession suitable for unit tests.

    The session is configured with two local threads and no console
    progress bars to reduce noise during testing.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def read_csv_with_schema(spark: SparkSession, path: str, schema) -> DataFrame:
    """Read a CSV file enforcing a schema (header expected)."""
    return (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(path)
    )


def coerce_impressions_from_json(df: DataFrame, col: str = COL_IMPRESSIONS) -> DataFrame:
    """
    Parse JSON strings into array<struct> for the impressions column.

    When reading CSVs, the impressions column may be loaded as a plain
    string containing JSON.  This helper detects string type and uses
    `from_json` to parse into the correct array of structs.  If the
    column is already an array, the DataFrame is returned unchanged.
    """
    dtype = dict(df.dtypes).get(col)
    if dtype is None:
        return df
    if dtype.startswith("string"):
        return df.withColumn(col, F.from_json(F.col(col), ArrayType(impression_struct)))
    return df
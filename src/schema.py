"""
Schema definitions for the pyspark‑coding‑challenge project.

Spark DataFrame schemas are defined here to ensure type safety
throughout the transformations.  The impressions schema uses
an array of struct types to represent multiple impressions per row.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    ArrayType,
    TimestampType,
    DateType,
)

# Nested struct describing a single impression entry
impression_struct = StructType([
    StructField("item_id", IntegerType(), True),
    StructField("is_order", BooleanType(), True),
])

# Schema for the impressions dataset
impressions_schema = StructType([
    StructField("dt", StringType(), True),             # date of impression in YYYY‑MM‑DD format
    StructField("ranking_id", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("impressions", ArrayType(impression_struct), True),
])

# Schema for the clicks dataset
clicks_schema = StructType([
    StructField("dt", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("click_time", TimestampType(), True),
])

# Schema for the add‑to‑carts dataset
add_to_carts_schema = StructType([
    StructField("dt", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("config_id", IntegerType(), True),  # alias for item_id
    StructField("simple_id", IntegerType(), True),
    StructField("occurred_at", TimestampType(), True),
])

# Schema for the previous orders dataset
previous_orders_schema = StructType([
    StructField("order_date", DateType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("config_id", IntegerType(), True),
    StructField("simple_id", IntegerType(), True),
])
"""
Basic smoke test for the pyspark‑coding‑challenge Day 1 implementation.

This test reads the small synthetic CSVs from the data directory,
normalizes the impressions JSON column if necessary, and executes the
pipeline end‑to‑end.  It asserts that the output contains the expected
columns and that the resulting sequences are padded to the configured
length.
"""

from pyspark.sql import functions as F

from src import schema, config
from src.utils import get_spark, read_csv_with_schema, coerce_impressions_from_json
from src.transformations import build_training_inputs


def test_load_and_build():
    spark = get_spark("test-load")

    # Load sample datasets
    imp = read_csv_with_schema(spark, config.IMPRESSIONS_PATH, schema.impressions_schema)
    clk = read_csv_with_schema(spark, config.CLICKS_PATH, schema.clicks_schema)
    atc = read_csv_with_schema(spark, config.ADD_TO_CARTS_PATH, schema.add_to_carts_schema)
    ords = read_csv_with_schema(spark, config.PREVIOUS_ORDERS_PATH, schema.previous_orders_schema)

    # Parse impressions JSON if necessary
    imp = coerce_impressions_from_json(imp)

    # Run pipeline
    out = build_training_inputs(imp, clk, atc, ords, lookback_days=365, max_seq=10)

    # Validate required columns
    for col in ["dt", "ranking_id", "customer_id", "impression_item_id", "is_order", "actions", "action_types"]:
        assert col in out.columns

    # Grab a sample row and validate sequence lengths
    row = out.limit(1).collect()[0]
    assert len(row.actions) == 10
    assert len(row.action_types) == 10

    spark.stop()
"""
Unit tests for the metrics functions in the pyspark‑coding‑challenge project.

These tests construct tiny in‑memory DataFrames to verify that the
`calculate_dq_metrics` function returns the expected keys.  The
numeric values themselves depend on the input data and are not
asserted here to keep the tests robust across minor changes in
implementation.
"""

from src import constants
from src.utils import get_spark
from src.metrics import calculate_dq_metrics


def test_calculate_dq_metrics_keys():
    spark = get_spark("test-metrics")
    # Create a very small DataFrame with actions arrays
    data = [
        ("2025-07-31", "r1", 1, 101, False, [101, 0, 0], [constants.ACTION_CLICK, constants.ACTION_PADDING, constants.ACTION_PADDING]),
        ("2025-07-31", "r2", 2, 103, False, [0, 0, 0], [constants.ACTION_PADDING, constants.ACTION_PADDING, constants.ACTION_PADDING]),
    ]
    schema = "dt string, ranking_id string, customer_id int, impression_item_id int, is_order boolean, actions array<int>, action_types array<byte>"
    df = spark.createDataFrame(data, schema=schema)
    metrics = calculate_dq_metrics(df)
    assert set(metrics.keys()) == {"zero_history_pct", "avg_history_length", "action_type_distribution"}
    spark.stop()
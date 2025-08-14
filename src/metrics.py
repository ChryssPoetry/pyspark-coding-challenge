"""
Data quality metrics for the pyspark‑coding‑challenge project.

This module exposes helper functions to compute simple statistics
about the historical action sequences produced by the pipeline.  The
metrics are designed to give insight into the sparsity of sequences
and the distribution of action types.

The functions are written in a pure Spark style and return Python
objects (e.g. floats and dictionaries) rather than DataFrames to
facilitate easy JSON serialisation from the CLI.
"""

from typing import Dict, Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def calculate_dq_metrics(df: DataFrame) -> Dict[str, Any]:
    """
    Compute basic data quality metrics on the output of
    ``build_training_inputs``.

    Parameters
    ----------
    df: DataFrame
        The DataFrame returned by ``build_training_inputs``.  It is
        assumed to contain ``actions`` (array<int>) and ``action_types``
        (array<byte>) columns.

    Returns
    -------
    dict
        A dictionary with the following keys:

        ``zero_history_pct``: float
            Fraction of rows where the entire actions array consists
            solely of padding (zeros).  These rows represent
            impressions with no historical actions in the look‑back
            window.

        ``avg_history_length``: float
            The average number of non‑zero entries in the actions
            array (i.e. the mean historical sequence length).

        ``action_type_distribution``: dict
            A mapping from action type code (string) to its relative
            frequency across all positions in ``action_types``.
    """
    # Total number of rows for percentage calculations
    total_rows = df.count() if df.isStreaming is False else None

    # Zero‑history rows: those where the maximum action id is 0
    zero_count = df.filter(F.array_max("actions") == 0).count()
    zero_history_pct = (zero_count / total_rows) if total_rows else None

    # Average number of non‑zero actions per row
    history_lengths = df.select(
        F.expr("aggregate(actions, 0, (acc, x) -> acc + CASE WHEN x != 0 THEN 1 ELSE 0 END) as hist_len")
    )
    avg_history_length = history_lengths.agg(F.avg("hist_len")).first()[0]

    # Distribution of action types across all positions
    # Explode and count each action type then normalise by total
    exploded = df.select(F.explode("action_types").alias("type"))
    type_counts = exploded.groupBy("type").count().collect()
    total_positions = sum(row["count"] for row in type_counts)
    action_type_distribution = {
        str(row["type"]): (row["count"] / total_positions) for row in type_counts
    }

    return {
        "zero_history_pct": zero_history_pct,
        "avg_history_length": avg_history_length,
        "action_type_distribution": action_type_distribution,
    }
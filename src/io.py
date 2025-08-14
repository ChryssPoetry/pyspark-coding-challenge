"""
Input/output helpers for the pyspark‑coding‑challenge project.

While the primary input format for Day 1 and Day 2 is CSV, this
module provides a convenient function for persisting the training
inputs to Parquet.  Parquet is a columnar storage format well suited
to big data workloads and downstream model training.
"""

from pyspark.sql import DataFrame


def save_training_inputs(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    """
    Save the training inputs DataFrame to a Parquet file or directory.

    Parameters
    ----------
    df: DataFrame
        The output of ``build_training_inputs`` to persist.
    path: str
        The target directory or file path.  If the path does not
        exist it will be created.
    mode: str, optional
        Write mode to use.  Defaults to ``overwrite`` which will
        replace existing data.  Other valid modes include ``append``.
    """
    df.write.mode(mode).parquet(path)
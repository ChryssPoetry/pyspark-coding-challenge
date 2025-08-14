"""
Command‑line interface for the pyspark‑coding‑challenge project.

This module exposes a simple CLI that reads the four raw datasets,
builds the fixed‑length training inputs, optionally computes data
quality metrics, and writes the results to Parquet.  Run

    python -m src.cli --help

to see usage instructions.
"""

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession

from . import config, schema
from .utils import read_csv_with_schema, coerce_impressions_from_json
from .transformations import build_training_inputs
from .metrics import calculate_dq_metrics
from .io import save_training_inputs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the PySpark coding challenge pipeline")
    parser.add_argument(
        "--impressions", default=config.IMPRESSIONS_PATH, help="Path to impressions CSV"
    )
    parser.add_argument(
        "--clicks", default=config.CLICKS_PATH, help="Path to clicks CSV"
    )
    parser.add_argument(
        "--add_to_carts", default=config.ADD_TO_CARTS_PATH, help="Path to add-to-carts CSV"
    )
    parser.add_argument(
        "--previous_orders", default=config.PREVIOUS_ORDERS_PATH, help="Path to previous orders CSV"
    )
    parser.add_argument(
        "--output", required=True, help="Directory to write the Parquet training inputs"
    )
    parser.add_argument(
        "--lookback_days", type=int, default=365, help="Look-back window in days"
    )
    parser.add_argument(
        "--max_seq", type=int, default=1000, help="Maximum sequence length"
    )
    parser.add_argument(
        "--metrics_json", help="Optional path to write data quality metrics as JSON"
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("PySparkChallengeCLI")
        .master("local[2]")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    # Read inputs
    imp = read_csv_with_schema(spark, args.impressions, schema.impressions_schema)
    imp = coerce_impressions_from_json(imp)
    clk = read_csv_with_schema(spark, args.clicks, schema.clicks_schema)
    atc = read_csv_with_schema(spark, args.add_to_carts, schema.add_to_carts_schema)
    ords = read_csv_with_schema(spark, args.previous_orders, schema.previous_orders_schema)

    # Build training inputs
    train_df = build_training_inputs(
        imp, clk, atc, ords, lookback_days=args.lookback_days, max_seq=args.max_seq
    )

    # Persist to Parquet
    save_training_inputs(train_df, args.output, mode="overwrite")

    # Compute and optionally write DQ metrics
    if args.metrics_json:
        metrics = calculate_dq_metrics(train_df)
        # Ensure output directory exists
        Path(args.metrics_json).parent.mkdir(parents=True, exist_ok=True)
        with open(args.metrics_json, "w", encoding="utf-8") as fh:
            json.dump(metrics, fh, indent=2)

    spark.stop()


if __name__ == "__main__":
    main()
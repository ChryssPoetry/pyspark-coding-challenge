"""
File system paths and aliases for the pyspark‑coding‑challenge project.

The `data` directory contains small synthetic CSV files for local
development and testing.  Should you wish to run the pipeline on
external datasets you can override these constants or pass custom
paths when invoking the CLI.
"""

import os

# Base directory points to the project root when this module lives
# under `src/`.
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

# Canonical CSV paths used in examples and tests
IMPRESSIONS_PATH = os.path.join(DATA_DIR, "impressions.csv")
CLICKS_PATH = os.path.join(DATA_DIR, "clicks.csv")
ADD_TO_CARTS_PATH = os.path.join(DATA_DIR, "add_to_carts.csv")
PREVIOUS_ORDERS_PATH = os.path.join(DATA_DIR, "previous_orders.csv")

# Larger synthetic datasets for Day 3 experiments.  These files
# contain approximately 1 000 impression rows and corresponding
# clicks, add‑to‑carts and orders.  They are optional and only
# referenced in the Day 3 examples and notebook.
IMPRESSIONS_PATH_1000 = os.path.join(DATA_DIR, "impressions_1000.csv")
CLICKS_PATH_1000 = os.path.join(DATA_DIR, "clicks_1000.csv")
ADD_TO_CARTS_PATH_1000 = os.path.join(DATA_DIR, "add_to_carts_1000.csv")
PREVIOUS_ORDERS_PATH_1000 = os.path.join(DATA_DIR, "previous_orders_1000.csv")

# Back‑compat aliases to support earlier naming schemes
ADDTOCART_PATH = ADD_TO_CARTS_PATH  # pylint: disable=invalid-name
ORDERS_PATH = PREVIOUS_ORDERS_PATH  # pylint: disable=invalid-name
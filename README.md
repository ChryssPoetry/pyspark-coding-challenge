# PySpark Coding Challenge

This repository implements a multi‑day **PySpark coding challenge**.  The
goal is to transform raw impression and user action datasets into
fixed‑length training sequences suitable for feeding a transformer
model, while incrementally adding tooling, metrics and analytical
features across three days.  Day 1 lays the foundation, Day 2 adds
a command‑line interface and metrics, and Day 3 scales the solution
to larger datasets with additional analytics.

On Day 1 the project focuses on:

* Designing strict Spark schemas for each dataset.
* Normalising heterogeneous action types into a single table.
* Building one row per impression by exploding arrays.
* Gathering up to a fixed number of historical actions per
  customer–impression pair with a configurable look‑back window.
* Padding or truncating sequences to a fixed length.

## Structure

```
pyspark-coding-challenge/
├── src/                     # Python source code
│   ├── __init__.py          # re‑exports for convenience
│   ├── constants.py         # action codes and defaults
│   ├── config.py            # data paths (synthetic data)
│   ├── schema.py            # Spark schemas
│   ├── utils.py             # session and parsing helpers
│   ├── transformations.py   # core logic (explode, normalise, build)
│   ├── io.py                # Parquet writer (Day 2)
│   ├── metrics.py           # data quality metrics (Day 2)
│   └── analysis.py          # advanced analytics (Day 3)
├── data/                    # synthetic CSVs
│   ├── impressions.csv
│   ├── clicks.csv
│   ├── add_to_carts.csv
│   ├── previous_orders.csv
│   ├── impressions_1000.csv  # larger dataset for Day 3
│   ├── clicks_1000.csv
│   ├── add_to_carts_1000.csv
│   └── previous_orders_1000.csv
├── tests/
│   ├── test_load.py         # basic smoke test (Day 1)
│   ├── test_metrics.py       # unit tests for metrics (Day 2)
│   └── test_cli.py           # tests for the CLI parser (Day 2)
├── notebooks/               # interactive experiments (Day 3)
│   └── day1_2_3_experiment.ipynb
├── requirements.txt         # PySpark and pytest
└── README.md
```

## Running

Create a virtual environment, install dependencies and run the tests:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest -q
```

To manually run the pipeline on the synthetic data:

```python
from pyspark.sql import SparkSession
from src import config, schema
from src.utils import read_csv_with_schema, coerce_impressions_from_json
from src.transformations import build_training_inputs

spark = SparkSession.builder.master("local[2]").getOrCreate()

imp = read_csv_with_schema(spark, config.IMPRESSIONS_PATH, schema.impressions_schema)
clk = read_csv_with_schema(spark, config.CLICKS_PATH, schema.clicks_schema)
atc = read_csv_with_schema(spark, config.ADD_TO_CARTS_PATH, schema.add_to_carts_schema)
ord = read_csv_with_schema(spark, config.PREVIOUS_ORDERS_PATH, schema.previous_orders_schema)

imp = coerce_impressions_from_json(imp)
out = build_training_inputs(imp, clk, atc, ord)
out.show(truncate=False)

spark.stop()
```

The resulting DataFrame will have one row per impression with
fixed‑length `actions` and `action_types` arrays of integers.

## Day 2 – Extended Functionality

Day 2 builds upon the foundations of Day 1 by adding a small
command‑line interface (CLI), data quality metrics and Parquet
export.  The goals for Day 2 are to make the pipeline easier to
integrate into larger workflows and to provide basic instrumentation
for data scientists to understand the output.

### New features

* **CLI runner** (`python -m src.cli`): A simple command‑line
  interface accepts paths to the four raw CSVs, the output
  directory, and optional parameters for the look‑back window and
  sequence length.  It reads the data, builds the training inputs
  and writes them to Parquet.  Optionally, it writes data quality
  metrics to a JSON file.

* **Parquet writer** (`save_training_inputs` in `src/io.py`):
  Encapsulates writing the training DataFrame to Parquet with a
  configurable write mode (default is `overwrite`).

* **Data quality metrics** (`calculate_dq_metrics` in
  `src/metrics.py`): Computes simple statistics on the
  training inputs, including:

  - `zero_history_pct`: Fraction of impressions with no historical
    actions (i.e. all zeros in the `actions` array).
  - `avg_history_length`: Average number of non‑zero items in the
    `actions` array.
  - `action_type_distribution`: Relative frequency of each action
    type across all positions in `action_types`.

* **Tests**: Added tests for the metrics module and CLI parser
  under `tests/`.  These tests ensure the new functions return
  expected keys and default values.

### Example usage

Run the full pipeline from the command line and write the results
to a Parquet directory:

```bash
python -m src.cli \
    --impressions data/impressions.csv \
    --clicks data/clicks.csv \
    --add_to_carts data/add_to_carts.csv \
    --previous_orders data/previous_orders.csv \
    --output output/training_inputs
```

To compute metrics and save them to a JSON file add the
`--metrics_json` flag:

```bash
python -m src.cli \
    --output output/training_inputs \
    --metrics_json output/metrics.json
```

### Notes for Day 2

* The CLI is intentionally lightweight.  It uses `argparse` from the
  standard library and does not depend on any external packages.
* Metrics computation triggers additional Spark jobs (e.g. a
  `count` and some aggregations).  For large datasets this may
  increase runtime.
* If you wish to integrate these components into other scripts,
  import from `src.transformations`, `src.io` and `src.metrics` as
  needed.

## Day 3 – Scaling and Advanced Analysis

Day 3 extends the project in two dimensions:

1. **Larger synthetic dataset** – To evaluate the scalability of the
   pipeline, we generated a second set of synthetic CSV files with
   approximately 1 000 impression rows.  These files live in the
   `data/` directory with a `_1000` suffix (e.g. `impressions_1000.csv`).
   You can pass these paths to the CLI or load them directly in
   Python to stress‑test the logic on a larger dataset.

2. **Advanced analytics** – A new module `src.analysis` introduces
   functions for computing the distribution of historical sequence
   lengths (`sequence_length_distribution`) and for analysing the
   per‑position frequency of each action type
   (`action_type_frequency_by_position`).  These analytics help
   identify sparsity (how many zeros) and bias (which action types
   dominate recent positions) in the training sequences.

### Running on the larger dataset

To run the pipeline on the 1 000‑row dataset and write the output to
Parquet, invoke the CLI with the new file names.  For example:

```bash
python -m src.cli \
    --impressions data/impressions_1000.csv \
    --clicks data/clicks_1000.csv \
    --add_to_carts data/add_to_carts_1000.csv \
    --previous_orders data/previous_orders_1000.csv \
    --output output/training_inputs_1000 \
    --metrics_json output/metrics_1000.json
```

Once the Parquet file is written you can load it with Spark or
use the metrics JSON to inspect sparsity and action type
distribution.

### Analysing sequence length and action type frequencies

From Python you can leverage the Day 3 analysis helpers to gain
additional insight into the sequences.  After constructing the
training DataFrame, call:

```python
from src.analysis import sequence_length_distribution, action_type_frequency_by_position

length_dist = sequence_length_distribution(train_df)
type_freqs = action_type_frequency_by_position(train_df, max_positions=5)
```

`length_dist` will be a dictionary mapping the number of non‑zero
historical actions to the count of impressions with that length.
`type_freqs` will be a list of dictionaries (one per position) mapping
action type codes to their relative frequency at that position.

### Notebook experiment

The repository includes a Jupyter notebook under `notebooks/` that
walks through Days 1–3.  It demonstrates how to run the pipeline on
both the small and large datasets, compute metrics and perform the
advanced analyses.  Open `notebooks/day1_2_3_experiment.ipynb` in
JupyterLab or VS Code to explore the pipeline interactively.

## Integrating with GitHub

To publish this project to your own GitHub repository, follow
these steps in a local clone of the directory:

```bash
# 1. Initialise a git repository (if one does not already exist)
git init

# 2. Add all files and commit with a descriptive message
git add .
git commit -m "Implement Days 1–3 of the PySpark coding challenge"

# 3. Create a new repository on GitHub (via the web UI) and copy
#    its HTTPS or SSH URL.  Then add it as a remote named 'origin':
git remote add origin https://github.com/username/pyspark-coding-challenge.git

# 4. Push the main branch to GitHub
git branch -M main
git push -u origin main
```

After pushing, your code and notebook will be visible in your
GitHub repository.  You can continue iterating locally and push
additional commits as you extend the analysis or adapt the
pipeline to real data.

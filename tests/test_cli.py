"""
Smoke tests for the CLI argument parser.

These tests verify that the CLI can parse required arguments and
populate defaults for optional parameters.  They do not run the full
Spark job to keep test execution fast.
"""

import sys

from src.cli import parse_args


def test_cli_parse_defaults(monkeypatch):
    # Provide minimal required arguments via sys.argv
    monkeypatch.setattr(sys, "argv", ["prog", "--output", "/tmp/out"])
    args = parse_args()
    assert args.output == "/tmp/out"
    assert args.lookback_days == 365
    assert args.max_seq == 1000
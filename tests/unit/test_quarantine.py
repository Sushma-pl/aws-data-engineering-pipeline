"""Unit tests for src/validation/quarantine.py — annotation and column-presence tests.

Covers Requirements 7.1, 7.2, 7.3.
"""
import pandas as pd
import pytest
import sys
import os

# Make conftest helpers importable as plain functions
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from conftest import make_valid_row

from src.validation.quarantine import add_validation_errors


class TestAddValidationErrors:

    # ------------------------------------------------------------------ #
    # Requirement 7.1 — negative fare_amount → "Negative fare amount"     #
    # ------------------------------------------------------------------ #
    def test_negative_fare_amount_annotated(self):
        """A row with fare_amount=-1.0 must have 'Negative fare amount' in validation_errors."""
        row = make_valid_row(fare_amount=-1.0)
        df = pd.DataFrame([row])

        result = add_validation_errors(df, source_file="test_file.parquet")

        assert result.loc[0, "validation_errors"] is not None, (
            "validation_errors should not be None for a row with negative fare_amount"
        )
        assert "Negative fare amount" in result.loc[0, "validation_errors"], (
            f"Expected 'Negative fare amount' in validation_errors, "
            f"got: {result.loc[0, 'validation_errors']!r}"
        )

    # ------------------------------------------------------------------ #
    # Requirement 7.2 — fully valid row → validation_errors is None       #
    # ------------------------------------------------------------------ #
    def test_valid_row_has_no_errors(self):
        """A fully valid row (no overrides) must have validation_errors = None."""
        row = make_valid_row()
        df = pd.DataFrame([row])

        result = add_validation_errors(df, source_file="test_file.parquet")

        assert result.loc[0, "validation_errors"] is None, (
            f"Expected validation_errors to be None for a valid row, "
            f"got: {result.loc[0, 'validation_errors']!r}"
        )

    # ------------------------------------------------------------------ #
    # Requirement 7.3 — output always has required columns;               #
    #                   rejection_timestamp is a non-empty ISO-8601 string #
    # ------------------------------------------------------------------ #
    def test_required_columns_present(self):
        """Output DataFrame must always contain source_file, validation_errors, rejection_timestamp."""
        row = make_valid_row()
        df = pd.DataFrame([row])

        result = add_validation_errors(df, source_file="my_source.parquet")

        for col in ("source_file", "validation_errors", "rejection_timestamp"):
            assert col in result.columns, f"Expected column '{col}' in output, found: {list(result.columns)}"

    def test_rejection_timestamp_is_nonempty_iso8601(self):
        """rejection_timestamp must be a non-empty ISO-8601 UTC string."""
        row = make_valid_row()
        df = pd.DataFrame([row])

        result = add_validation_errors(df, source_file="my_source.parquet")

        ts = result.loc[0, "rejection_timestamp"]
        assert isinstance(ts, str) and len(ts) > 0, (
            f"rejection_timestamp must be a non-empty string, got: {ts!r}"
        )
        # Minimal ISO-8601 sanity check: contains a 'T' separator and a timezone marker
        assert "T" in ts, f"rejection_timestamp does not look like ISO-8601: {ts!r}"
        # UTC timestamps end with '+00:00' or 'Z'
        assert ts.endswith("+00:00") or ts.endswith("Z"), (
            f"rejection_timestamp is not UTC: {ts!r}"
        )

    def test_source_file_value_propagated(self):
        """The source_file column value must match the argument passed to add_validation_errors."""
        row = make_valid_row()
        df = pd.DataFrame([row])
        expected_source = "yellow_tripdata_2026-05.parquet"

        result = add_validation_errors(df, source_file=expected_source)

        assert result.loc[0, "source_file"] == expected_source, (
            f"Expected source_file='{expected_source}', got: {result.loc[0, 'source_file']!r}"
        )

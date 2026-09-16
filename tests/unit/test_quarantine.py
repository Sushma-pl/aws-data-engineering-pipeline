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


from src.validation.quarantine import write_rejected_file


class TestWriteRejectedFile:
    """Tests for write_rejected_file — round-trip persistence and multi-error joining.

    Covers Requirements 7.4, 7.6.
    """

    # ------------------------------------------------------------------ #
    # Requirement 7.4 — written file can be read back with correct shape  #
    # ------------------------------------------------------------------ #
    def test_write_rejected_file_round_trip(self, tmp_path):
        """Write a 2-row annotated DataFrame, read it back, assert row count and required columns."""
        rows = [
            make_valid_row(fare_amount=-1.0),
            make_valid_row(total_amount=-2.0),
        ]
        df = pd.DataFrame(rows)
        annotated = add_validation_errors(df, source_file="test_source.parquet")

        output_path = tmp_path / "rejected.parquet"
        returned_path = write_rejected_file(annotated, str(output_path))

        # Returned path should point to an existing file
        assert returned_path.exists(), "write_rejected_file should return a Path to an existing file"

        # Round-trip: read back and check shape
        read_back = pd.read_parquet(returned_path)
        assert len(read_back) == 2, (
            f"Expected 2 rows after round-trip, got {len(read_back)}"
        )

        for col in ("source_file", "validation_errors", "rejection_timestamp"):
            assert col in read_back.columns, (
                f"Expected column '{col}' in round-tripped DataFrame, found: {list(read_back.columns)}"
            )

    # ------------------------------------------------------------------ #
    # Requirement 7.6 — multiple errors on one row are joined with ", "   #
    # ------------------------------------------------------------------ #
    def test_multi_error_row_joins_messages(self, tmp_path):
        """A row with both fare_amount<0 and total_amount<0 should have both messages joined by ', '."""
        row = make_valid_row(fare_amount=-1.0, total_amount=-1.0)
        df = pd.DataFrame([row])

        result = add_validation_errors(df, source_file="multi_error.parquet")

        errors = result.loc[0, "validation_errors"]
        assert errors is not None, "validation_errors should not be None for a row with multiple errors"
        assert "Negative fare amount" in errors, (
            f"Expected 'Negative fare amount' in validation_errors, got: {errors!r}"
        )
        assert "Negative total amount" in errors, (
            f"Expected 'Negative total amount' in validation_errors, got: {errors!r}"
        )
        # Both messages must be joined by ", "
        assert "Negative fare amount" in errors and "Negative total amount" in errors, (
            f"Both error messages should appear in: {errors!r}"
        )
        assert ", " in errors, (
            f"Multiple errors should be joined by ', ', got: {errors!r}"
        )


# ======================================================================
# Requirement 7.5 — Property 8: Quarantine Row-Count Invariant
# ======================================================================

class TestQuarantineRowCountInvariant:
    """add_validation_errors must return exactly as many rows as the input DataFrame."""

    def test_row_count_invariant_valid_rows(self):
        """5 all-valid rows in → 5 rows out."""
        df = pd.DataFrame([make_valid_row() for _ in range(5)])

        result = add_validation_errors(df, source_file="test_file.parquet")

        assert len(result) == 5, (
            f"Expected 5 rows in output, got {len(result)}"
        )

    def test_row_count_invariant_mixed_rows(self):
        """4 rows (2 valid + 2 invalid) in → 4 rows out."""
        valid_rows   = [make_valid_row() for _ in range(2)]
        invalid_rows = [
            make_valid_row(fare_amount=-1.0),   # triggers "Negative fare amount"
            make_valid_row(trip_distance=-5.0), # triggers "Negative trip distance"
        ]
        df = pd.DataFrame(valid_rows + invalid_rows)

        result = add_validation_errors(df, source_file="test_file.parquet")

        assert len(result) == 4, (
            f"Expected 4 rows in output, got {len(result)}"
        )

    def test_row_count_invariant_empty_df(self):
        """Empty DataFrame in → empty DataFrame out (0 rows)."""
        df = pd.DataFrame(columns=list(make_valid_row().keys()))

        result = add_validation_errors(df, source_file="test_file.parquet")

        assert len(result) == 0, (
            f"Expected 0 rows in output for empty input, got {len(result)}"
        )

"""
Unit tests for src/validation/quality_checks.py — hard-failure rules.

Task 7.1: One test per hard-failure rule (14 total).
Each test builds a single-row DataFrame using make_valid_row() with exactly
one field mutated to trigger a specific hard-failure rule, then asserts:
  - The row appears in invalid_df  (len == 1)
  - The row does NOT appear in valid_df  (len == 0)

Requirements: 6.1, 6.2
"""
import sys
import os

import pandas as pd
import pytest

# Allow importing make_valid_row from tests/conftest.py when pytest is not
# the import driver (e.g. direct python invocation). During a normal pytest
# run conftest fixtures/helpers are available on the path automatically.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from conftest import make_valid_row

from src.validation.quality_checks import run_quality_checks


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _single_row_df(**overrides) -> pd.DataFrame:
    """Return a one-row DataFrame with the given field(s) overridden."""
    return pd.DataFrame([make_valid_row(**overrides)])


def _assert_invalid(df: pd.DataFrame) -> None:
    """Assert the mutated row is caught as invalid and not valid."""
    valid_df, invalid_df, _ = run_quality_checks(df)
    assert len(invalid_df) == 1, (
        f"Expected 1 invalid row, got {len(invalid_df)}"
    )
    assert len(valid_df) == 0, (
        f"Expected 0 valid rows, got {len(valid_df)}"
    )


# ---------------------------------------------------------------------------
# Hard-failure rule tests
# ---------------------------------------------------------------------------

class TestHardFailureRules:

    # 1. Negative trip_distance → negative_trip_distance
    def test_negative_trip_distance(self):
        df = _single_row_df(trip_distance=-1.0)
        _assert_invalid(df)

    # 2. Negative fare_amount → negative_fare
    def test_negative_fare_amount(self):
        df = _single_row_df(fare_amount=-5.0)
        _assert_invalid(df)

    # 3. Negative total_amount → negative_total
    def test_negative_total_amount(self):
        df = _single_row_df(total_amount=-1.0)
        _assert_invalid(df)

    # 4. passenger_count = 0 (≤ 0) → invalid_passenger_count
    def test_passenger_count_zero(self):
        df = _single_row_df(passenger_count=0)
        _assert_invalid(df)

    # 4b. passenger_count negative also triggers invalid_passenger_count
    def test_passenger_count_negative(self):
        df = _single_row_df(passenger_count=-1)
        _assert_invalid(df)

    # 5. tpep_pickup_datetime > tpep_dropoff_datetime → pickup_after_dropoff
    def test_pickup_after_dropoff(self):
        df = _single_row_df(
            tpep_pickup_datetime=pd.Timestamp("2026-05-01 09:00:00"),
            tpep_dropoff_datetime=pd.Timestamp("2026-05-01 08:00:00"),
        )
        _assert_invalid(df)

    # 6. PULocationID = 0 (≤ 0) → invalid_PULocationID
    def test_pu_location_id_zero(self):
        df = _single_row_df(PULocationID=0)
        _assert_invalid(df)

    # 7. DOLocationID = 0 (≤ 0) → invalid_DOLocationID
    def test_do_location_id_zero(self):
        df = _single_row_df(DOLocationID=0)
        _assert_invalid(df)

    # 8. Null VendorID → null_VendorID
    def test_null_vendor_id(self):
        df = _single_row_df(VendorID=None)
        _assert_invalid(df)

    # 9. Null tpep_pickup_datetime → null_pickup_datetime
    def test_null_pickup_datetime(self):
        df = _single_row_df(tpep_pickup_datetime=None)
        _assert_invalid(df)

    # 10. Null tpep_dropoff_datetime → null_dropoff_datetime
    def test_null_dropoff_datetime(self):
        df = _single_row_df(tpep_dropoff_datetime=None)
        _assert_invalid(df)

    # 11. Null PULocationID → null_PULocationID
    def test_null_pu_location_id(self):
        df = _single_row_df(PULocationID=None)
        _assert_invalid(df)

    # 12. Null DOLocationID → null_DOLocationID
    def test_null_do_location_id(self):
        df = _single_row_df(DOLocationID=None)
        _assert_invalid(df)

    # 13. Null fare_amount → null_fare_amount
    def test_null_fare_amount(self):
        df = _single_row_df(fare_amount=None)
        _assert_invalid(df)

    # 14. Null total_amount → null_total_amount
    def test_null_total_amount(self):
        df = _single_row_df(total_amount=None)
        _assert_invalid(df)


# ---------------------------------------------------------------------------
# Row-count invariant and all-valid input tests
# Requirements: 6.3, 6.4
# ---------------------------------------------------------------------------

class TestRowCountInvariant:
    """
    Verify that valid_rows + invalid_rows == total_rows for any input,
    and that a fully-valid DataFrame produces zero invalid rows.
    """

    def test_row_count_invariant_single_valid_row(self):
        """1 valid row: valid + invalid counts must equal total."""
        df = _single_row_df()
        _, _, quality_report = run_quality_checks(df)
        assert (
            quality_report["valid_rows"] + quality_report["invalid_rows"]
            == quality_report["total_rows"]
        ), (
            f"Row-count invariant broken: "
            f"{quality_report['valid_rows']} + {quality_report['invalid_rows']} "
            f"!= {quality_report['total_rows']}"
        )

    def test_row_count_invariant_mixed_rows(self):
        """3 rows (1 valid, 2 invalid): valid + invalid counts must equal total."""
        df = pd.concat(
            [
                _single_row_df(),
                _single_row_df(fare_amount=-1.0),
                _single_row_df(VendorID=None),
            ],
            ignore_index=True,
        )
        _, _, quality_report = run_quality_checks(df)
        assert (
            quality_report["valid_rows"] + quality_report["invalid_rows"]
            == quality_report["total_rows"]
        ), (
            f"Row-count invariant broken: "
            f"{quality_report['valid_rows']} + {quality_report['invalid_rows']} "
            f"!= {quality_report['total_rows']}"
        )

    def test_all_valid_input_produces_empty_invalid_df(self):
        """10 fully-valid rows: invalid_df must be empty, valid_df must have 10 rows."""
        df = pd.DataFrame([make_valid_row() for _ in range(10)])
        valid_df, invalid_df, _ = run_quality_checks(df)
        assert len(invalid_df) == 0, (
            f"Expected 0 invalid rows, got {len(invalid_df)}"
        )
        assert len(valid_df) == 10, (
            f"Expected 10 valid rows, got {len(valid_df)}"
        )


# ---------------------------------------------------------------------------
# Warning-level check tests  (Task 7.3 — Requirements 6.5)
# ---------------------------------------------------------------------------

class TestWarningChecks:
    """
    Verify that warning-level conditions:
      - do NOT remove the row from valid_df
      - are counted correctly in quality_report["warnings"]

    Requirements: 6.5
    """

    def test_null_passenger_count_is_warning(self):
        """passenger_count=None → row stays valid; null_passenger_count warning == 1."""
        df = _single_row_df(passenger_count=None)
        valid_df, invalid_df, quality_report = run_quality_checks(df)

        assert len(valid_df) == 1, (
            f"Row with null passenger_count should remain valid, got {len(valid_df)} valid rows"
        )
        assert len(invalid_df) == 0, (
            f"Row with null passenger_count should not be invalid, got {len(invalid_df)} invalid rows"
        )
        assert quality_report["warnings"]["null_passenger_count"] == 1, (
            f"Expected null_passenger_count warning == 1, got {quality_report['warnings']['null_passenger_count']}"
        )

    def test_zero_trip_distance_is_warning(self):
        """trip_distance=0.0 → row stays valid; zero_trip_distance warning == 1."""
        df = _single_row_df(trip_distance=0.0)
        valid_df, invalid_df, quality_report = run_quality_checks(df)

        assert len(valid_df) == 1, (
            f"Row with zero trip_distance should remain valid, got {len(valid_df)} valid rows"
        )
        assert len(invalid_df) == 0, (
            f"Row with zero trip_distance should not be invalid, got {len(invalid_df)} invalid rows"
        )
        assert quality_report["warnings"]["zero_trip_distance"] == 1, (
            f"Expected zero_trip_distance warning == 1, got {quality_report['warnings']['zero_trip_distance']}"
        )

    def test_zero_duration_is_warning(self):
        """pickup == dropoff → row stays valid; zero_duration warning == 1."""
        df = _single_row_df(
            tpep_pickup_datetime=pd.Timestamp("2026-05-01 08:00:00"),
            tpep_dropoff_datetime=pd.Timestamp("2026-05-01 08:00:00"),
        )
        valid_df, invalid_df, quality_report = run_quality_checks(df)

        assert len(valid_df) == 1, (
            f"Row with zero duration should remain valid, got {len(valid_df)} valid rows"
        )
        assert len(invalid_df) == 0, (
            f"Row with zero duration should not be invalid, got {len(invalid_df)} invalid rows"
        )
        assert quality_report["warnings"]["zero_duration"] == 1, (
            f"Expected zero_duration warning == 1, got {quality_report['warnings']['zero_duration']}"
        )

    def test_duration_over_24h_is_warning(self):
        """dropoff = pickup + 25 hours → row stays valid; duration_over_24_hours warning == 1."""
        df = _single_row_df(
            tpep_dropoff_datetime=pd.Timestamp("2026-05-02 09:00:00"),
        )
        valid_df, invalid_df, quality_report = run_quality_checks(df)

        assert len(valid_df) == 1, (
            f"Row with >24h duration should remain valid, got {len(valid_df)} valid rows"
        )
        assert len(invalid_df) == 0, (
            f"Row with >24h duration should not be invalid, got {len(invalid_df)} invalid rows"
        )
        assert quality_report["warnings"]["duration_over_24_hours"] == 1, (
            f"Expected duration_over_24_hours warning == 1, got {quality_report['warnings']['duration_over_24_hours']}"
        )


# ---------------------------------------------------------------------------
# Task 7.4 — hard_failures counter tests
# Requirements: 6.6
# ---------------------------------------------------------------------------

class TestHardFailureCounters:
    """
    Each test mutates exactly one field and asserts:
      - The corresponding hard_failures counter equals 1.
      - All other hard_failures counters remain 0.
    """

    def _all_zero_except(self, counters: dict, expected_key: str) -> None:
        """Assert that only expected_key is 1 and every other counter is 0."""
        for key, value in counters.items():
            if key == expected_key:
                assert value == 1, (
                    f"Expected hard_failures['{expected_key}'] == 1, got {value}"
                )
            else:
                assert value == 0, (
                    f"Expected hard_failures['{key}'] == 0, got {value} "
                    f"(only '{expected_key}' should be non-zero)"
                )

    # 1. negative fare_amount → "negative_fare" == 1
    def test_negative_fare_counter(self):
        df = _single_row_df(fare_amount=-5.0)
        _, _, quality_report = run_quality_checks(df)
        counters = quality_report["hard_failures"]
        assert counters["negative_fare"] == 1
        self._all_zero_except(counters, "negative_fare")

    # 2. null VendorID → "null_VendorID" == 1
    def test_null_vendor_id_counter(self):
        df = _single_row_df(VendorID=None)
        _, _, quality_report = run_quality_checks(df)
        counters = quality_report["hard_failures"]
        assert counters["null_VendorID"] == 1
        self._all_zero_except(counters, "null_VendorID")

    # 3. pickup > dropoff → "pickup_after_dropoff" == 1
    def test_pickup_after_dropoff_counter(self):
        df = _single_row_df(
            tpep_pickup_datetime=pd.Timestamp("2026-05-01 09:00:00"),
            tpep_dropoff_datetime=pd.Timestamp("2026-05-01 08:00:00"),
        )
        _, _, quality_report = run_quality_checks(df)
        counters = quality_report["hard_failures"]
        assert counters["pickup_after_dropoff"] == 1
        self._all_zero_except(counters, "pickup_after_dropoff")

    # 4. passenger_count = 0 → "invalid_passenger_count" == 1
    def test_invalid_passenger_count_counter(self):
        df = _single_row_df(passenger_count=0)
        _, _, quality_report = run_quality_checks(df)
        counters = quality_report["hard_failures"]
        assert counters["invalid_passenger_count"] == 1
        self._all_zero_except(counters, "invalid_passenger_count")

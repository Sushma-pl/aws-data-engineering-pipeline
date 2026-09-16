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

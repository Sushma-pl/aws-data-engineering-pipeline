"""
Shared pytest fixtures and helpers.

make_valid_row() is a plain function (not a fixture) so any test can call it
with keyword overrides to inject specific bad values:

    make_valid_row(fare_amount=-5.0)   # inject a negative fare
    make_valid_row(VendorID=None)      # inject a null VendorID
"""
import pandas as pd
import pytest


def make_valid_row(**overrides) -> dict:
    """Return a dict representing a fully valid taxi trip row."""
    row = {
        "VendorID": 1,
        "tpep_pickup_datetime":  pd.Timestamp("2026-05-01 08:00:00"),
        "tpep_dropoff_datetime": pd.Timestamp("2026-05-01 08:30:00"),
        "passenger_count":       2,
        "trip_distance":         3.5,
        "RatecodeID":            1,
        "store_and_fwd_flag":    "N",
        "PULocationID":          100,
        "DOLocationID":          200,
        "payment_type":          1,
        "fare_amount":           15.0,
        "extra":                 0.5,
        "mta_tax":               0.5,
        "tip_amount":            3.0,
        "tolls_amount":          0.0,
        "improvement_surcharge": 0.3,
        "total_amount":          19.8,
        "congestion_surcharge":  2.5,
        "Airport_fee":           0.0,
        "cbd_congestion_fee":    0.0,
    }
    row.update(overrides)
    return row


@pytest.fixture
def valid_df():
    """Single valid row as a DataFrame."""
    return pd.DataFrame([make_valid_row()])


@pytest.fixture
def valid_df_10():
    """Ten identical valid rows as a DataFrame."""
    return pd.DataFrame([make_valid_row() for _ in range(10)])


@pytest.fixture
def sample_source_file(tmp_path):
    """Write a valid single-row Parquet file to tmp_path, return its path string."""
    df = pd.DataFrame([make_valid_row()])
    p = tmp_path / "yellow_tripdata_2026-05.parquet"
    df.to_parquet(p, index=False)
    return str(p)

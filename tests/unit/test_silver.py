"""
Unit tests for src/transformation/silver.py

Task 10.1 — session-scoped spark fixture and column-rename tests
Requirements: 9.1, 9.6
"""
import pytest
from pyspark.sql import SparkSession

import sys
import os

# Ensure project root is on sys.path so conftest helpers are importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tests.conftest import make_valid_row
from src.transformation.silver import transform_to_silver


# ---------------------------------------------------------------------------
# Session-scoped SparkSession fixture — no S3 / MinIO configuration
# (Requirement 9.6)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    """Local SparkSession with no S3 config for Silver unit tests."""
    session = (
        SparkSession.builder
        .master("local[*]")
        .appName("test_silver")
        .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _make_silver_input(spark: SparkSession, **overrides):
    """Build a single-row Spark DataFrame from make_valid_row() using Bronze column names."""
    row = make_valid_row(**overrides)
    return spark.createDataFrame([row])


# ---------------------------------------------------------------------------
# Task 10.1 — Column rename tests (Requirements 9.1, 9.6)
# ---------------------------------------------------------------------------

class TestColumnRenames:
    """Verify that transform_to_silver renames the five Bronze column names
    to their Silver equivalents and that the original Bronze names are absent."""

    RENAMED = {
        "VendorID":     "vendor_id",
        "RatecodeID":   "ratecode_id",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
        "Airport_fee":  "airport_fee",
    }

    def test_silver_columns_present(self, spark):
        """All five renamed column names must appear in silver_df."""
        input_df = _make_silver_input(spark)
        silver_df, _ = transform_to_silver(input_df)
        silver_columns = silver_df.columns

        for bronze_name, silver_name in self.RENAMED.items():
            assert silver_name in silver_columns, (
                f"Expected renamed column '{silver_name}' (from '{bronze_name}') "
                f"to be present in silver_df, but it was not found. "
                f"Actual columns: {silver_columns}"
            )

    def test_original_bronze_columns_absent(self, spark):
        """None of the five original Bronze column names must appear in silver_df."""
        input_df = _make_silver_input(spark)
        silver_df, _ = transform_to_silver(input_df)
        silver_columns = silver_df.columns

        for bronze_name in self.RENAMED:
            assert bronze_name not in silver_columns, (
                f"Original Bronze column '{bronze_name}' should have been renamed "
                f"but is still present in silver_df. "
                f"Actual columns: {silver_columns}"
            )

    def test_both_renames_in_single_assertion(self, spark):
        """Compound assertion: renamed present AND originals absent — single DataFrame build."""
        input_df = _make_silver_input(spark)
        silver_df, _ = transform_to_silver(input_df)
        silver_columns = set(silver_df.columns)

        missing_silver = [s for b, s in self.RENAMED.items() if s not in silver_columns]
        leftover_bronze = [b for b in self.RENAMED if b in silver_columns]

        assert not missing_silver, f"Missing Silver column(s): {missing_silver}"
        assert not leftover_bronze, f"Unrenamed Bronze column(s) still present: {leftover_bronze}"

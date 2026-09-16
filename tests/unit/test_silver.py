"""
Unit tests for src/transformation/silver.py

Task 10.1 — session-scoped spark fixture and column-rename tests
Requirements: 9.1, 9.6
"""
import pytest
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, LongType, StringType, DoubleType, TimestampType,
)

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
    import sys as _sys
    session = (
        SparkSession.builder
        .master("local[*]")
        .appName("test_silver")
        .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8")
        # On Windows the bare `python` command may resolve to the Microsoft Store
        # stub.  Explicitly set the interpreter so Spark worker processes start.
        .config("spark.pyspark.python", _sys.executable)
        .config("spark.pyspark.driver.python", _sys.executable)
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _make_silver_input(spark: SparkSession, **overrides):
    """Build a single-row Spark DataFrame from make_valid_row() using Bronze column names.

    pd.Timestamp objects in the dict cannot be reliably inferred as TimestampType
    by spark.createDataFrame when no schema is supplied (they may be inferred as
    STRUCT<> on some Python/PySpark versions).  We therefore cast the two datetime
    columns explicitly after creation.
    """
    from pyspark.sql import functions as F

    row = make_valid_row(**overrides)
    df = spark.createDataFrame([row])
    # Cast the timestamp columns to TimestampType so transform_to_silver can use them
    df = df.withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast("timestamp"))
    df = df.withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast("timestamp"))
    return df


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


# ---------------------------------------------------------------------------
# Module-level spark session ref for hypothesis tests (Task 10.3)
# ---------------------------------------------------------------------------

_spark_session = None


@pytest.fixture(scope="session", autouse=True)
def _store_spark_session(spark):
    """Store the session-scoped SparkSession in a module-level variable so
    hypothesis tests (which cannot receive pytest fixtures) can access it."""
    global _spark_session
    _spark_session = spark


# ---------------------------------------------------------------------------
# Task 10.2 — Derived column and rejection logic tests
# Requirements: 9.2, 9.3, 9.4
# ---------------------------------------------------------------------------

class TestDerivedColumns:
    """Verify trip_duration_minutes computation, fare rejection, and all-valid path."""

    def test_trip_duration_minutes_computed_correctly(self, spark):
        """trip_duration_minutes must equal (dropoff - pickup) / 60 within 0.01 min.
        (Requirement 9.2)
        """
        import datetime as dt
        pickup = pd.Timestamp("2026-05-01 08:00:00")
        dropoff = pd.Timestamp("2026-05-01 08:45:00")
        row = make_valid_row(
            tpep_pickup_datetime=pickup,
            tpep_dropoff_datetime=dropoff,
        )
        input_df = spark.createDataFrame([row])
        silver_df, _ = transform_to_silver(input_df)

        result = silver_df.select("trip_duration_minutes").collect()
        assert len(result) == 1, "Expected one row in silver_df"

        expected = (dropoff - pickup).total_seconds() / 60.0
        actual = float(result[0]["trip_duration_minutes"])
        assert abs(actual - expected) <= 0.01, (
            f"trip_duration_minutes expected {expected:.4f}, got {actual:.4f}"
        )

    def test_negative_fare_row_lands_in_rejected_not_silver(self, spark):
        """A row with fare_amount < 0 must appear in rejected_df, not silver_df.
        (Requirement 9.3)
        """
        row = make_valid_row(fare_amount=-5.0)
        input_df = spark.createDataFrame([row])
        silver_df, rejected_df = transform_to_silver(input_df)

        assert silver_df.count() == 0, (
            "silver_df should be empty when fare_amount is negative"
        )
        assert rejected_df.count() == 1, (
            "rejected_df should contain the row with negative fare_amount"
        )

    def test_rejection_reason_is_nonempty_string(self, spark):
        """rejection_reason for a rejected row must be a non-empty string.
        (Requirement 9.3)
        """
        row = make_valid_row(fare_amount=-5.0)
        input_df = spark.createDataFrame([row])
        _, rejected_df = transform_to_silver(input_df)

        result = rejected_df.select("rejection_reason").collect()
        assert len(result) == 1
        rejection_reason = result[0]["rejection_reason"]
        assert isinstance(rejection_reason, str) and len(rejection_reason) >= 1, (
            f"rejection_reason must be a non-empty string, got: {rejection_reason!r}"
        )

    def test_all_valid_rows_produce_empty_rejected_df(self, spark):
        """All valid rows (from make_valid_row with no overrides) must produce
        an empty rejected_df. (Requirement 9.4)
        """
        from pyspark.sql import functions as F
        rows = [make_valid_row() for _ in range(5)]
        input_df = spark.createDataFrame(rows)
        # Cast timestamp columns to ensure correct type inference
        input_df = (
            input_df
            .withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast("timestamp"))
            .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast("timestamp"))
        )
        silver_df, rejected_df = transform_to_silver(input_df)

        assert rejected_df.count() == 0, (
            "rejected_df should be empty when all rows are valid"
        )
        assert silver_df.count() == 5, (
            "silver_df should contain all 5 valid rows"
        )


# ---------------------------------------------------------------------------
# Task 10.3 — Silver row-count invariant property test
# Feature: aws-data-pipeline-completion, Property 9: Silver Transformation Row-Count Invariant
# Requirements: 9.5
# ---------------------------------------------------------------------------

from hypothesis import given, settings, HealthCheck
from hypothesis import strategies as st


# Explicit Bronze schema used by the Hypothesis-generated DataFrames.
# Without this, spark.createDataFrame may infer pd.Timestamp or nullable
# integer columns as STRUCT<> / wrong types and fail inside transform_to_silver.
_BRONZE_SCHEMA = StructType([
    StructField("VendorID",              LongType(),      True),
    StructField("tpep_pickup_datetime",  TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count",       LongType(),      True),
    StructField("trip_distance",         DoubleType(),    False),
    StructField("RatecodeID",            LongType(),      False),
    StructField("store_and_fwd_flag",    StringType(),    False),
    StructField("PULocationID",          LongType(),      True),
    StructField("DOLocationID",          LongType(),      True),
    StructField("payment_type",          LongType(),      False),
    StructField("fare_amount",           DoubleType(),    True),
    StructField("extra",                 DoubleType(),    False),
    StructField("mta_tax",               DoubleType(),    False),
    StructField("tip_amount",            DoubleType(),    False),
    StructField("tolls_amount",          DoubleType(),    False),
    StructField("improvement_surcharge", DoubleType(),    False),
    StructField("total_amount",          DoubleType(),    True),
    StructField("congestion_surcharge",  DoubleType(),    False),
    StructField("Airport_fee",           DoubleType(),    False),
    StructField("cbd_congestion_fee",    DoubleType(),    False),
])


def _silver_row_strategy():
    """Generate a single valid-row tuple matching _BRONZE_SCHEMA with randomly varied key fields."""
    return st.fixed_dictionaries({
        "VendorID":              st.one_of(st.just(None), st.integers(min_value=1, max_value=2)),
        "tpep_pickup_datetime":  st.datetimes(
                                     min_value=pd.Timestamp("2020-01-01").to_pydatetime(),
                                     max_value=pd.Timestamp("2026-12-31").to_pydatetime(),
                                 ).map(pd.Timestamp),
        "tpep_dropoff_datetime": st.datetimes(
                                     min_value=pd.Timestamp("2020-01-01").to_pydatetime(),
                                     max_value=pd.Timestamp("2026-12-31").to_pydatetime(),
                                 ).map(pd.Timestamp),
        "passenger_count":       st.one_of(st.just(None), st.integers(min_value=-1, max_value=6)),
        "trip_distance":         st.floats(min_value=-1.0, max_value=50.0, allow_nan=False),
        "RatecodeID":            st.integers(min_value=1, max_value=6),
        "store_and_fwd_flag":    st.just("N"),
        "PULocationID":          st.one_of(st.just(None), st.integers(min_value=-1, max_value=265)),
        "DOLocationID":          st.one_of(st.just(None), st.integers(min_value=-1, max_value=265)),
        "payment_type":          st.integers(min_value=1, max_value=6),
        "fare_amount":           st.one_of(st.just(None), st.floats(min_value=-10.0, max_value=200.0, allow_nan=False)),
        "extra":                 st.floats(min_value=0.0, max_value=5.0, allow_nan=False),
        "mta_tax":               st.floats(min_value=0.0, max_value=1.0, allow_nan=False),
        "tip_amount":            st.floats(min_value=0.0, max_value=50.0, allow_nan=False),
        "tolls_amount":          st.floats(min_value=0.0, max_value=20.0, allow_nan=False),
        "improvement_surcharge": st.floats(min_value=0.0, max_value=1.0, allow_nan=False),
        "total_amount":          st.one_of(st.just(None), st.floats(min_value=-5.0, max_value=300.0, allow_nan=False)),
        "congestion_surcharge":  st.floats(min_value=0.0, max_value=3.0, allow_nan=False),
        "Airport_fee":           st.floats(min_value=0.0, max_value=10.0, allow_nan=False),
        "cbd_congestion_fee":    st.floats(min_value=0.0, max_value=5.0, allow_nan=False),
    }).map(lambda d: (
        d["VendorID"],
        d["tpep_pickup_datetime"].to_pydatetime(),
        d["tpep_dropoff_datetime"].to_pydatetime(),
        d["passenger_count"],
        d["trip_distance"],
        d["RatecodeID"],
        d["store_and_fwd_flag"],
        d["PULocationID"],
        d["DOLocationID"],
        d["payment_type"],
        d["fare_amount"],
        d["extra"],
        d["mta_tax"],
        d["tip_amount"],
        d["tolls_amount"],
        d["improvement_surcharge"],
        d["total_amount"],
        d["congestion_surcharge"],
        d["Airport_fee"],
        d["cbd_congestion_fee"],
    ))


class TestSilverRowCountInvariant:
    """
    Property 9: Silver Transformation Row-Count Invariant
    silver_df.count() + rejected_df.count() == input.count()
    Validates: Requirements 9.5
    """

    @given(rows=st.lists(_silver_row_strategy(), min_size=1, max_size=50))
    @settings(max_examples=20, deadline=None, suppress_health_check=[HealthCheck.too_slow])
    def test_silver_row_count_invariant(self, rows):
        """
        **Property 9: Silver Transformation Row-Count Invariant**
        **Validates: Requirements 9.5**

        For any input DataFrame, silver_df.count() + rejected_df.count()
        must equal the input row count.
        """
        spark = _spark_session
        assert spark is not None, "Spark session not initialised"

        input_df = spark.createDataFrame(rows, schema=_BRONZE_SCHEMA)
        silver_df, rejected_df = transform_to_silver(input_df)

        input_count = len(rows)
        output_count = silver_df.count() + rejected_df.count()

        assert output_count == input_count, (
            f"Row-count invariant violated: silver({silver_df.count()}) + "
            f"rejected({rejected_df.count()}) = {output_count} != input({input_count})"
        )

"""
Unit tests for src/gold/gold.py

Task 11.1 — session-scoped spark fixture and daily revenue tests
Requirements: 10.1
"""
import datetime
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType, DateType, TimestampType,
)

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from src.gold.gold import compute_daily_revenue


# ---------------------------------------------------------------------------
# Session-scoped SparkSession fixture — no S3 / MinIO configuration
# (Requirement 9.6 style; required here to avoid network I/O)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    """Local SparkSession with no S3 config for Gold unit tests."""
    import sys as _sys
    session = (
        SparkSession.builder
        .master("local[*]")
        .appName("test_gold")
        .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8")
        # On Windows the bare `python` command may resolve to the Microsoft Store
        # stub.  Point Spark workers at the actual interpreter that is running
        # this test process so that PySpark worker processes start successfully.
        .config("spark.pyspark.python", _sys.executable)
        .config("spark.pyspark.driver.python", _sys.executable)
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Silver schema used to build in-memory test DataFrames
# ---------------------------------------------------------------------------

SILVER_SCHEMA = StructType([
    StructField("vendor_id",                IntegerType(),   True),
    StructField("tpep_pickup_datetime",      TimestampType(), True),
    StructField("tpep_dropoff_datetime",     TimestampType(), True),
    StructField("passenger_count",           IntegerType(),   True),
    StructField("trip_distance",             DoubleType(),    True),
    StructField("ratecode_id",               IntegerType(),   True),
    StructField("store_and_fwd_flag",        StringType(),    True),
    StructField("pickup_location_id",        IntegerType(),   True),
    StructField("dropoff_location_id",       IntegerType(),   True),
    StructField("payment_type",              IntegerType(),   True),
    StructField("fare_amount",               DoubleType(),    True),
    StructField("extra",                     DoubleType(),    True),
    StructField("mta_tax",                   DoubleType(),    True),
    StructField("tip_amount",                DoubleType(),    True),
    StructField("tolls_amount",              DoubleType(),    True),
    StructField("improvement_surcharge",     DoubleType(),    True),
    StructField("total_amount",              DoubleType(),    True),
    StructField("congestion_surcharge",      DoubleType(),    True),
    StructField("airport_fee",               DoubleType(),    True),
    StructField("cbd_congestion_fee",        DoubleType(),    True),
    StructField("trip_duration_minutes",     DoubleType(),    True),
    StructField("pickup_date",               DateType(),      True),
    StructField("pickup_hour",               IntegerType(),   True),
])


def _make_silver_row(
    fare_amount=15.0,
    tip_amount=3.0,
    total_amount=19.8,
    trip_distance=3.5,
    trip_duration_minutes=30.0,
    pickup_location_id=100,
    dropoff_location_id=200,
    pickup_date=None,
    pickup_hour=8,
):
    """Return a tuple matching SILVER_SCHEMA for in-memory test DataFrames."""
    if pickup_date is None:
        pickup_date = datetime.date(2026, 5, 1)
    pickup_dt = datetime.datetime(
        pickup_date.year, pickup_date.month, pickup_date.day, pickup_hour, 0, 0
    )
    dropoff_dt = pickup_dt + datetime.timedelta(minutes=trip_duration_minutes)
    return (
        1,                    # vendor_id
        pickup_dt,            # tpep_pickup_datetime
        dropoff_dt,           # tpep_dropoff_datetime
        2,                    # passenger_count
        trip_distance,        # trip_distance
        1,                    # ratecode_id
        "N",                  # store_and_fwd_flag
        pickup_location_id,   # pickup_location_id
        dropoff_location_id,  # dropoff_location_id
        1,                    # payment_type
        fare_amount,          # fare_amount
        0.5,                  # extra
        0.5,                  # mta_tax
        tip_amount,           # tip_amount
        0.0,                  # tolls_amount
        0.3,                  # improvement_surcharge
        total_amount,         # total_amount
        2.5,                  # congestion_surcharge
        0.0,                  # airport_fee
        0.0,                  # cbd_congestion_fee
        trip_duration_minutes,# trip_duration_minutes
        pickup_date,          # pickup_date
        pickup_hour,          # pickup_hour
    )


# ---------------------------------------------------------------------------
# Task 11.1 — Daily revenue: total_fare_amount == sum of input fare_amounts
# (Requirement 10.1)
# ---------------------------------------------------------------------------

class TestComputeDailyRevenue:
    """Verify compute_daily_revenue produces correct fare-amount totals."""

    def test_total_fare_amount_equals_sum_of_inputs(self, spark):
        """total_fare_amount must equal the exact sum of non-null fare_amount
        values in the input, rounded to 2 decimal places. (Requirement 10.1)
        """
        fares = [10.50, 7.25, 22.00, 5.75]          # known values, same date
        rows = [_make_silver_row(fare_amount=f) for f in fares]
        input_df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)

        result_df = compute_daily_revenue(input_df)
        result = result_df.collect()

        assert len(result) == 1, (
            f"Expected 1 output row (single pickup_date), got {len(result)}"
        )

        expected_total = round(sum(fares), 2)
        actual_total   = round(float(result[0]["total_fare_amount"]), 2)

        assert actual_total == expected_total, (
            f"total_fare_amount mismatch: expected {expected_total}, got {actual_total}"
        )

    def test_total_fare_amount_excludes_null_fares(self, spark):
        """Null fare_amount values must be excluded from total_fare_amount sum."""
        non_null_fares = [12.00, 8.50]
        rows = [
            _make_silver_row(fare_amount=f) for f in non_null_fares
        ] + [
            _make_silver_row(fare_amount=None)   # null — should not be summed
        ]
        input_df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)

        result_df = compute_daily_revenue(input_df)
        result = result_df.collect()

        assert len(result) == 1
        expected_total = round(sum(non_null_fares), 2)
        actual_total   = round(float(result[0]["total_fare_amount"]), 2)

        assert actual_total == expected_total, (
            f"Null fares should be excluded: expected {expected_total}, got {actual_total}"
        )

from src.gold.gold import compute_zone_metrics, compute_hourly_demand
from hypothesis import given, settings, HealthCheck
from hypothesis import strategies as st


# ---------------------------------------------------------------------------
# Module-level spark session ref for hypothesis tests (Tasks 11.3–11.6)
# ---------------------------------------------------------------------------

_spark_session = None


@pytest.fixture(scope="session", autouse=True)
def _store_spark_session(spark):
    """Store the session-scoped SparkSession in a module-level variable so
    hypothesis tests can access it without receiving a pytest fixture."""
    global _spark_session
    _spark_session = spark


# ---------------------------------------------------------------------------
# Task 11.2 — Zone metrics and hourly demand output-shape tests
# Requirements: 10.2, 10.3, 10.6
# ---------------------------------------------------------------------------

class TestComputeZoneMetrics:
    """Verify compute_zone_metrics produces correct output shape."""

    def test_row_count_equals_distinct_date_zone_combinations(self, spark):
        """Row count must equal distinct (pickup_date, pickup_location_id) combos.
        (Requirement 10.2)
        """
        rows = [
            _make_silver_row(pickup_date=datetime.date(2026, 5, 1), pickup_location_id=100),
            _make_silver_row(pickup_date=datetime.date(2026, 5, 1), pickup_location_id=200),
            _make_silver_row(pickup_date=datetime.date(2026, 5, 2), pickup_location_id=100),
            _make_silver_row(pickup_date=datetime.date(2026, 5, 1), pickup_location_id=100),  # duplicate combo
        ]
        input_df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result_df = compute_zone_metrics(input_df)

        # 3 distinct combos: (5/1,100), (5/1,200), (5/2,100)
        assert result_df.count() == 3, (
            f"Expected 3 rows for 3 distinct (pickup_date, pickup_location_id) combos, "
            f"got {result_df.count()}"
        )

    def test_output_contains_expected_metric_columns(self, spark):
        """Zone metrics output must have trip_count, avg_fare_amount, avg_trip_distance,
        avg_trip_duration_minutes columns."""
        rows = [_make_silver_row()]
        input_df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result_df = compute_zone_metrics(input_df)

        expected_cols = {"pickup_date", "pickup_location_id", "trip_count",
                         "avg_fare_amount", "avg_trip_distance", "avg_trip_duration_minutes"}
        actual_cols = set(result_df.columns)
        assert expected_cols.issubset(actual_cols), (
            f"Missing columns: {expected_cols - actual_cols}"
        )

    def test_single_row_produces_one_zone_metric_row(self, spark):
        """A single-row input must produce exactly one zone metric row."""
        rows = [_make_silver_row()]
        input_df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result_df = compute_zone_metrics(input_df)

        assert result_df.count() == 1


class TestComputeHourlyDemand:
    """Verify compute_hourly_demand output shape and filtering."""

    def test_row_count_equals_distinct_date_hour_combinations(self, spark):
        """Row count must equal distinct (pickup_date, pickup_hour) combos
        where pickup_hour in [0, 23]. (Requirement 10.3)
        """
        rows = [
            _make_silver_row(pickup_date=datetime.date(2026, 5, 1), pickup_hour=8),
            _make_silver_row(pickup_date=datetime.date(2026, 5, 1), pickup_hour=9),
            _make_silver_row(pickup_date=datetime.date(2026, 5, 2), pickup_hour=8),
            _make_silver_row(pickup_date=datetime.date(2026, 5, 1), pickup_hour=8),  # duplicate combo
        ]
        input_df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result_df = compute_hourly_demand(input_df)

        # 3 distinct combos: (5/1,8), (5/1,9), (5/2,8)
        assert result_df.count() == 3, (
            f"Expected 3 rows for 3 distinct (pickup_date, pickup_hour) combos, "
            f"got {result_df.count()}"
        )

    def test_rows_with_pickup_hour_outside_range_excluded(self, spark):
        """Rows with pickup_hour outside [0, 23] must be excluded from hourly demand.
        (Requirement 10.6)
        """
        valid_row = _make_silver_row(pickup_hour=10)
        invalid_hour_row = _make_silver_row(pickup_hour=25)  # outside [0,23]
        none_hour_row_data = list(_make_silver_row())
        # Build a row tuple with pickup_hour = None by copying and overriding
        row_list = list(_make_silver_row(pickup_hour=10))
        # Create rows with out-of-range hours
        rows = [valid_row, invalid_hour_row]
        input_df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result_df = compute_hourly_demand(input_df)

        # Only the valid-hour row should appear
        assert result_df.count() == 1, (
            f"Expected 1 row (pickup_hour=10), got {result_df.count()}. "
            "Rows with pickup_hour outside [0,23] should be excluded."
        )

    def test_null_pickup_hour_rows_excluded(self, spark):
        """Rows with null pickup_hour must be excluded from hourly demand output."""
        from pyspark.sql import Row
        valid_row = _make_silver_row(pickup_hour=8)
        # Build a null-hour row as dict for createDataFrame
        null_row = _make_silver_row(pickup_hour=8)
        rows = spark.createDataFrame([valid_row], schema=SILVER_SCHEMA)
        null_df = spark.createDataFrame(
            [(None,)], schema="pickup_hour INT"
        )
        # Simpler: just use two valid rows (one with hour None via schema override)
        # We'll test via SQL to set null directly
        rows_df = spark.createDataFrame([valid_row, valid_row], schema=SILVER_SCHEMA)
        # Set one row's pickup_hour to null
        from pyspark.sql import functions as Func
        rows_with_null = rows_df.withColumn(
            "pickup_hour",
            Func.when(Func.monotonically_increasing_id() == 0, Func.lit(None).cast("int"))
               .otherwise(Func.col("pickup_hour"))
        )

        result_df = compute_hourly_demand(rows_with_null)
        # The null-hour row should be excluded; only 1 row remains
        assert result_df.count() == 1, (
            "Row with null pickup_hour should be excluded from hourly demand"
        )

    def test_output_contains_expected_metric_columns(self, spark):
        """Hourly demand output must have trip_count and avg_fare_amount columns."""
        rows = [_make_silver_row()]
        input_df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result_df = compute_hourly_demand(input_df)

        expected_cols = {"pickup_date", "pickup_hour", "trip_count", "avg_fare_amount"}
        actual_cols = set(result_df.columns)
        assert expected_cols.issubset(actual_cols), (
            f"Missing columns: {expected_cols - actual_cols}"
        )


# ---------------------------------------------------------------------------
# Gold hypothesis strategy
# ---------------------------------------------------------------------------

def _gold_row_strategy():
    """Generate a row tuple matching SILVER_SCHEMA for property-based Gold tests."""
    return st.builds(
        lambda fare, tip, total, dist, dur, loc, date_offset, hour: _make_silver_row(
            fare_amount=fare,
            tip_amount=tip,
            total_amount=total,
            trip_distance=dist,
            trip_duration_minutes=dur,
            pickup_location_id=loc,
            pickup_date=datetime.date(2026, 1, 1) + datetime.timedelta(days=date_offset),
            pickup_hour=hour,
        ),
        fare=st.one_of(st.just(None), st.floats(min_value=0.0, max_value=200.0, allow_nan=False)),
        tip=st.floats(min_value=0.0, max_value=50.0, allow_nan=False),
        total=st.floats(min_value=0.0, max_value=300.0, allow_nan=False),
        dist=st.floats(min_value=0.0, max_value=50.0, allow_nan=False),
        dur=st.floats(min_value=1.0, max_value=120.0, allow_nan=False),
        loc=st.integers(min_value=1, max_value=265),
        date_offset=st.integers(min_value=0, max_value=30),
        hour=st.integers(min_value=0, max_value=23),
    )


def _gold_row_with_any_hour_strategy():
    """Like _gold_row_strategy but allows pickup_hour outside [0, 23]."""
    return st.builds(
        lambda fare, loc, date_offset, hour: _make_silver_row(
            fare_amount=fare,
            pickup_location_id=loc,
            pickup_date=datetime.date(2026, 1, 1) + datetime.timedelta(days=date_offset),
            pickup_hour=hour,
        ),
        fare=st.floats(min_value=0.0, max_value=200.0, allow_nan=False),
        loc=st.integers(min_value=1, max_value=265),
        date_offset=st.integers(min_value=0, max_value=30),
        hour=st.integers(min_value=-5, max_value=30),  # some outside [0,23]
    )


# ---------------------------------------------------------------------------
# Task 11.3 — Daily revenue row-count invariant (Property 1)
# Feature: aws-data-pipeline-completion, Property 1: Daily Revenue Row-Count Invariant
# Requirements: 1.6, 10.4
# ---------------------------------------------------------------------------

class TestDailyRevenueRowCountInvariant:
    """
    Property 1: Daily Revenue Row-Count Invariant
    Sum of total_trip_count across all rows == input row count.
    Validates: Requirements 1.6, 10.4
    """

    @given(rows=st.lists(_gold_row_strategy(), min_size=1, max_size=50))
    @settings(max_examples=20, deadline=None, suppress_health_check=[HealthCheck.too_slow])
    def test_total_trip_count_sum_equals_input_row_count(self, rows):
        """
        **Property 1: Daily Revenue Row-Count Invariant**
        **Validates: Requirements 1.6**

        Sum of total_trip_count across all pickup_date groups must equal
        the total number of input rows.
        """
        spark = _spark_session
        assert spark is not None, "Spark session not initialised"

        input_df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result_df = compute_daily_revenue(input_df)

        from pyspark.sql import functions as Func
        total_trip_count = result_df.agg(
            Func.sum("total_trip_count").alias("s")
        ).collect()[0]["s"]

        assert total_trip_count == len(rows), (
            f"Row-count invariant violated: sum(total_trip_count)={total_trip_count} "
            f"!= input_count={len(rows)}"
        )


# ---------------------------------------------------------------------------
# Task 11.4 — Zone metrics row-count invariant (Property 2)
# Feature: aws-data-pipeline-completion, Property 2: Zone Metrics Row-Count Invariant
# Requirements: 2.4, 10.5
# ---------------------------------------------------------------------------

class TestZoneMetricsRowCountInvariant:
    """
    Property 2: Zone Metrics Row-Count Invariant
    Sum of trip_count across all (pickup_date, pickup_location_id) groups == input row count.
    Validates: Requirements 2.4, 10.5
    """

    @given(rows=st.lists(_gold_row_strategy(), min_size=1, max_size=50))
    @settings(max_examples=20, deadline=None, suppress_health_check=[HealthCheck.too_slow])
    def test_trip_count_sum_equals_input_row_count(self, rows):
        """
        **Property 2: Zone Metrics Row-Count Invariant**
        **Validates: Requirements 2.4**

        Sum of trip_count across all zone groups must equal the total
        number of input rows.
        """
        spark = _spark_session
        assert spark is not None, "Spark session not initialised"

        input_df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result_df = compute_zone_metrics(input_df)

        from pyspark.sql import functions as Func
        total_trip_count = result_df.agg(
            Func.sum("trip_count").alias("s")
        ).collect()[0]["s"]

        assert total_trip_count == len(rows), (
            f"Zone metrics row-count invariant violated: sum(trip_count)={total_trip_count} "
            f"!= input_count={len(rows)}"
        )


# ---------------------------------------------------------------------------
# Task 11.5 — Hourly demand row-count invariant (Property 3)
# Feature: aws-data-pipeline-completion, Property 3: Hourly Demand Row-Count Invariant with Filter
# Requirements: 3.4, 3.5
# ---------------------------------------------------------------------------

class TestHourlyDemandRowCountInvariant:
    """
    Property 3: Hourly Demand Row-Count Invariant with Filter
    Sum of trip_count == count of input rows where pickup_hour in [0, 23] and non-null.
    Validates: Requirements 3.4, 3.5
    """

    @given(rows=st.lists(_gold_row_with_any_hour_strategy(), min_size=1, max_size=50))
    @settings(max_examples=20, deadline=None, suppress_health_check=[HealthCheck.too_slow])
    def test_trip_count_sum_equals_valid_hour_row_count(self, rows):
        """
        **Property 3: Hourly Demand Row-Count Invariant with Filter**
        **Validates: Requirements 3.4, 3.5**

        Sum of trip_count must equal the count of input rows where
        pickup_hour is in [0, 23] and non-null.
        """
        spark = _spark_session
        assert spark is not None, "Spark session not initialised"

        input_df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result_df = compute_hourly_demand(input_df)

        # Count input rows with valid pickup_hour
        expected_count = sum(
            1 for r in rows
            if r[22] is not None and 0 <= r[22] <= 23  # index 22 = pickup_hour in SILVER_SCHEMA
        )

        if expected_count == 0:
            assert result_df.count() == 0
            return

        from pyspark.sql import functions as Func
        total_trip_count = result_df.agg(
            Func.sum("trip_count").alias("s")
        ).collect()[0]["s"] or 0

        assert total_trip_count == expected_count, (
            f"Hourly demand row-count invariant violated: sum(trip_count)={total_trip_count} "
            f"!= valid_hour_row_count={expected_count}"
        )


# ---------------------------------------------------------------------------
# Task 11.6 — Gold aggregation sum consistency (Property 10)
# Feature: aws-data-pipeline-completion, Property 10: Gold Aggregation Sum Consistency
# Requirements: 10.4
# ---------------------------------------------------------------------------

class TestGoldAggregationSumConsistency:
    """
    Property 10: Gold Aggregation Sum Consistency
    Sum of total_fare_amount in daily revenue == sum of fare_amount in input (within 0.01).
    Validates: Requirements 10.4
    """

    @given(rows=st.lists(
        st.builds(
            lambda fare, date_offset: _make_silver_row(
                fare_amount=fare,
                pickup_date=datetime.date(2026, 1, 1) + datetime.timedelta(days=date_offset),
            ),
            fare=st.floats(min_value=0.0, max_value=200.0, allow_nan=False, allow_infinity=False),
            date_offset=st.integers(min_value=0, max_value=30),
        ),
        min_size=1, max_size=50,
    ))
    @settings(max_examples=20, deadline=None, suppress_health_check=[HealthCheck.too_slow])
    def test_total_fare_amount_sum_matches_input(self, rows):
        """
        **Property 10: Gold Aggregation Sum Consistency**
        **Validates: Requirements 10.4**

        Sum of total_fare_amount across all pickup_date groups in the daily
        revenue output must equal the sum of fare_amount in the input,
        within an absolute tolerance of 0.01.
        """
        spark = _spark_session
        assert spark is not None, "Spark session not initialised"

        input_df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        result_df = compute_daily_revenue(input_df)

        # Expected: sum of non-null fare_amount values from input rows
        # Each row in rows has fare_amount at index 10 per SILVER_SCHEMA
        expected_fare_sum = sum(r[10] for r in rows if r[10] is not None)

        from pyspark.sql import functions as Func
        actual_fare_sum = float(
            result_df.agg(Func.sum("total_fare_amount").alias("s")).collect()[0]["s"] or 0.0
        )

        assert abs(actual_fare_sum - expected_fare_sum) <= 0.01, (
            f"Fare sum mismatch: gold_sum={actual_fare_sum:.4f}, "
            f"input_sum={expected_fare_sum:.4f}, diff={abs(actual_fare_sum - expected_fare_sum):.6f}"
        )

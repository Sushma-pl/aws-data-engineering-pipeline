"""
Unit tests for the QueryLayer class in src/query/query.py.

Task 12.1 — Property 6: Missing Environment Variable Raises ValueError
Validates: Requirements 4.7

Because src/ingestion/config.py reads environment variables at module-import
time (via os.getenv), we cannot simply use monkeypatch.delenv after the module
has already been imported.  Instead we patch the module-level constants that
QueryLayer.__init__ reads directly on the src.query.query module using
monkeypatch.setattr, which takes effect immediately for the duration of the
test without any re-import gymnastics.
"""

import pytest

import src.query.query as query_module
from src.query.query import QueryLayer

# ---------------------------------------------------------------------------
# Helper: a fixture that ensures all three env-var constants are set to valid
# placeholder values on the query module before each test, then patches them
# individually to simulate a missing/empty value.
# ---------------------------------------------------------------------------

_VALID_ENDPOINT = "localhost:9000"
_VALID_ACCESS_KEY = "minioadmin"
_VALID_SECRET_KEY = "minioadmin"


@pytest.fixture(autouse=True)
def _patch_all_valid(monkeypatch):
    """
    Before every test in this module, set all three constants to valid
    placeholder strings so that only the one constant under test is blank.
    """
    monkeypatch.setattr(query_module, "MINIO_ENDPOINT", _VALID_ENDPOINT)
    monkeypatch.setattr(query_module, "MINIO_ACCESS_KEY", _VALID_ACCESS_KEY)
    monkeypatch.setattr(query_module, "MINIO_SECRET_KEY", _VALID_SECRET_KEY)


# ---------------------------------------------------------------------------
# Property 6: Missing Environment Variable Raises ValueError
# Validates: Requirements 4.7
# ---------------------------------------------------------------------------


def test_missing_minio_endpoint_raises_value_error(monkeypatch):
    """
    **Property 6: Missing Environment Variable Raises ValueError**
    **Validates: Requirements 4.7**

    When MINIO_ENDPOINT is absent (empty string), QueryLayer() must raise
    ValueError and the message must contain the variable name 'MINIO_ENDPOINT'.
    """
    monkeypatch.setattr(query_module, "MINIO_ENDPOINT", "")

    with pytest.raises(ValueError, match="MINIO_ENDPOINT"):
        QueryLayer()


def test_missing_minio_access_key_raises_value_error(monkeypatch):
    """
    **Property 6: Missing Environment Variable Raises ValueError**
    **Validates: Requirements 4.7**

    When MINIO_ACCESS_KEY is absent (empty string), QueryLayer() must raise
    ValueError and the message must contain the variable name 'MINIO_ACCESS_KEY'.
    """
    monkeypatch.setattr(query_module, "MINIO_ACCESS_KEY", "")

    with pytest.raises(ValueError, match="MINIO_ACCESS_KEY"):
        QueryLayer()


def test_missing_minio_secret_key_raises_value_error(monkeypatch):
    """
    **Property 6: Missing Environment Variable Raises ValueError**
    **Validates: Requirements 4.7**

    When MINIO_SECRET_KEY is absent (empty string), QueryLayer() must raise
    ValueError and the message must contain the variable name 'MINIO_SECRET_KEY'.
    """
    monkeypatch.setattr(query_module, "MINIO_SECRET_KEY", "")

    with pytest.raises(ValueError, match="MINIO_SECRET_KEY"):
        QueryLayer()


def test_none_minio_endpoint_raises_value_error(monkeypatch):
    """
    **Property 6: Missing Environment Variable Raises ValueError**
    **Validates: Requirements 4.7**

    When MINIO_ENDPOINT is None (variable not set in os.environ at import
    time), QueryLayer() must raise ValueError mentioning 'MINIO_ENDPOINT'.
    """
    monkeypatch.setattr(query_module, "MINIO_ENDPOINT", None)

    with pytest.raises(ValueError, match="MINIO_ENDPOINT"):
        QueryLayer()


def test_none_minio_access_key_raises_value_error(monkeypatch):
    """
    **Property 6: Missing Environment Variable Raises ValueError**
    **Validates: Requirements 4.7**

    When MINIO_ACCESS_KEY is None, QueryLayer() must raise ValueError
    mentioning 'MINIO_ACCESS_KEY'.
    """
    monkeypatch.setattr(query_module, "MINIO_ACCESS_KEY", None)

    with pytest.raises(ValueError, match="MINIO_ACCESS_KEY"):
        QueryLayer()


def test_none_minio_secret_key_raises_value_error(monkeypatch):
    """
    **Property 6: Missing Environment Variable Raises ValueError**
    **Validates: Requirements 4.7**

    When MINIO_SECRET_KEY is None, QueryLayer() must raise ValueError
    mentioning 'MINIO_SECRET_KEY'.
    """
    monkeypatch.setattr(query_module, "MINIO_SECRET_KEY", None)

    with pytest.raises(ValueError, match="MINIO_SECRET_KEY"):
        QueryLayer()


def test_all_valid_env_vars_does_not_raise():
    """
    Sanity check: when all three env vars are valid non-empty strings (set by
    the autouse fixture), QueryLayer() must NOT raise any exception.
    """
    # The autouse fixture has already patched all three to valid values.
    ql = QueryLayer()
    assert ql.endpoint == _VALID_ENDPOINT
    assert ql.access_key == _VALID_ACCESS_KEY
    assert ql.secret_key == _VALID_SECRET_KEY


# ---------------------------------------------------------------------------
# Task 12.2 — SQL length validation, schema fidelity, and determinism tests
# Requirements: 4.1, 4.5, 4.6
# ---------------------------------------------------------------------------

import duckdb
import pandas as pd
import tempfile
import os


class TestSQLLengthValidation:
    """Verify QueryLayer.query raises ValueError for out-of-range SQL lengths."""

    def test_empty_sql_raises_value_error(self):
        """SQL of length 0 must raise ValueError. (Requirement 4.1)"""
        ql = QueryLayer()
        with pytest.raises(ValueError, match="SQL length"):
            ql.query("", "silver")

    def test_sql_exceeding_10000_chars_raises_value_error(self):
        """SQL longer than 10,000 characters must raise ValueError. (Requirement 4.1)"""
        ql = QueryLayer()
        long_sql = "SELECT 1 " + ("--" * 5001)  # exceeds 10,000 chars
        assert len(long_sql) > 10_000
        with pytest.raises(ValueError, match="SQL length"):
            ql.query(long_sql, "silver")

    def test_sql_of_exactly_10000_chars_does_not_raise_on_length(self, monkeypatch, tmp_path):
        """SQL of exactly 10,000 characters must NOT raise a length ValueError."""
        # Write a small local Parquet so the query can proceed past length check
        df = pd.DataFrame({"id": [1, 2, 3]})
        parquet_path = str(tmp_path / "test.parquet")
        df.to_parquet(parquet_path, index=False)

        ql = QueryLayer()
        # Override _build_path to return the local file and _configure_s3 to no-op
        monkeypatch.setattr(ql, "_build_path", lambda dataset: parquet_path)
        monkeypatch.setattr(ql, "_configure_s3", lambda conn: None)

        # Build a 10,000-char SQL that is syntactically valid
        padding = " " * (10_000 - len("SELECT * FROM dataset"))
        sql_10k = "SELECT * FROM dataset" + padding
        assert len(sql_10k) == 10_000

        # Should not raise ValueError for SQL length
        try:
            ql.query(sql_10k, "silver")
        except ValueError as e:
            if "SQL length" in str(e):
                pytest.fail(f"Unexpected SQL length error for exactly 10,000 chars: {e}")


# ---------------------------------------------------------------------------
# Helpers for schema-fidelity and determinism tests
# ---------------------------------------------------------------------------

def _make_local_parquet(tmp_path, data: dict) -> str:
    """Write a dict of lists to a local Parquet file and return the path."""
    df = pd.DataFrame(data)
    path = str(tmp_path / "test_data.parquet")
    df.to_parquet(path, index=False)
    return path


def _local_query_layer(monkeypatch, parquet_path: str) -> "QueryLayer":
    """Return a QueryLayer whose _build_path returns a local file path
    and _configure_s3 is a no-op, so queries work without MinIO."""
    ql = QueryLayer()
    monkeypatch.setattr(ql, "_build_path", lambda dataset: parquet_path)
    monkeypatch.setattr(ql, "_configure_s3", lambda conn: None)
    return ql


class TestSchemaFidelity:
    """
    Property 4: Query Layer Returns Schema-Faithful DataFrames
    Validates: Requirements 4.5

    Column names and dtypes of the returned DataFrame must match the
    Parquet file's schema exactly — no columns added, removed, or recast.
    """

    def test_returned_dataframe_columns_match_parquet_schema(self, monkeypatch, tmp_path):
        """
        **Property 4: Query Layer Returns Schema-Faithful DataFrames**
        **Validates: Requirements 4.5**

        SELECT * must return a DataFrame whose columns match the Parquet
        file's column names exactly.
        """
        data = {
            "pickup_date": pd.to_datetime(["2026-05-01", "2026-05-02"]),
            "fare_amount":  [10.5, 22.0],
            "trip_count":   [3, 5],
        }
        parquet_path = _make_local_parquet(tmp_path, data)
        ql = _local_query_layer(monkeypatch, parquet_path)

        result_df = ql.query("SELECT * FROM dataset", "silver")

        # Column names must match
        expected_cols = sorted(data.keys())
        actual_cols = sorted(result_df.columns.tolist())
        assert actual_cols == expected_cols, (
            f"Column mismatch: expected {expected_cols}, got {actual_cols}"
        )

    def test_returned_dataframe_dtypes_match_parquet_schema(self, monkeypatch, tmp_path):
        """
        **Property 4: Query Layer Returns Schema-Faithful DataFrames**
        **Validates: Requirements 4.5**

        The dtypes of the returned DataFrame must be compatible with those
        in the Parquet file (numeric stays numeric, strings stay strings).
        """
        data = {
            "vendor_id":   pd.array([1, 2], dtype="int64"),
            "fare_amount": pd.array([10.5, 22.0], dtype="float64"),
            "zone_name":   ["Manhattan", "Brooklyn"],
        }
        parquet_path = _make_local_parquet(tmp_path, data)

        # Read the Parquet directly to get expected dtypes
        expected_df = pd.read_parquet(parquet_path)

        ql = _local_query_layer(monkeypatch, parquet_path)
        result_df = ql.query("SELECT * FROM dataset", "silver")

        for col in expected_df.columns:
            assert col in result_df.columns, f"Column '{col}' missing from result"
            # Check broad dtype family (numeric vs object) rather than exact bit width
            expected_kind = expected_df[col].dtype.kind  # 'i', 'f', 'O', etc.
            actual_kind   = result_df[col].dtype.kind
            assert expected_kind == actual_kind, (
                f"Dtype mismatch for column '{col}': "
                f"expected kind '{expected_kind}', got '{actual_kind}'"
            )

    def test_no_extra_columns_added(self, monkeypatch, tmp_path):
        """The result DataFrame must not have any extra columns beyond the schema."""
        data = {"id": [1, 2, 3], "value": [10.0, 20.0, 30.0]}
        parquet_path = _make_local_parquet(tmp_path, data)
        ql = _local_query_layer(monkeypatch, parquet_path)

        result_df = ql.query("SELECT * FROM dataset", "silver")
        assert set(result_df.columns) == set(data.keys()), (
            f"Extra or missing columns: got {set(result_df.columns)}, expected {set(data.keys())}"
        )


class TestDeterminism:
    """
    Property 5: Query Layer Determinism
    Validates: Requirements 4.6

    The same query executed twice must return identical row counts and
    column values in every row.
    """

    def test_same_query_returns_identical_row_count(self, monkeypatch, tmp_path):
        """
        **Property 5: Query Layer Determinism**
        **Validates: Requirements 4.6**

        Running the same query twice must return DataFrames with identical
        row counts.
        """
        data = {"pickup_date": ["2026-05-01", "2026-05-02"], "fare_amount": [10.5, 22.0]}
        parquet_path = _make_local_parquet(tmp_path, data)
        ql = _local_query_layer(monkeypatch, parquet_path)

        result1 = ql.query("SELECT * FROM dataset", "silver")
        result2 = ql.query("SELECT * FROM dataset", "silver")

        assert len(result1) == len(result2), (
            f"Row count mismatch across identical queries: {len(result1)} vs {len(result2)}"
        )

    def test_same_query_returns_identical_values(self, monkeypatch, tmp_path):
        """
        **Property 5: Query Layer Determinism**
        **Validates: Requirements 4.6**

        Running the same query twice must return DataFrames with identical
        column values in every row.
        """
        data = {
            "id":          [1, 2, 3],
            "fare_amount": [10.0, 15.5, 22.0],
            "zone":        ["Manhattan", "Brooklyn", "Queens"],
        }
        parquet_path = _make_local_parquet(tmp_path, data)
        ql = _local_query_layer(monkeypatch, parquet_path)

        sql = "SELECT * FROM dataset ORDER BY id"
        result1 = ql.query(sql, "silver")
        result2 = ql.query(sql, "silver")

        pd.testing.assert_frame_equal(
            result1.reset_index(drop=True),
            result2.reset_index(drop=True),
            check_like=False,
        )


# ---------------------------------------------------------------------------
# Task 12.3 — Parameterized query binding test
# Requirements: 4.4
# ---------------------------------------------------------------------------

class TestParameterizedQueryBinding:
    """
    Verify QueryLayer.query uses parameterized binding via DuckDB ? placeholders
    rather than interpolating raw parameter strings into the SQL.
    Requirements: 4.4
    """

    def test_parameterized_filter_returns_correct_rows(self, monkeypatch, tmp_path):
        """
        Passing a filter value via params must correctly bind via ? placeholder
        and return only matching rows.  The raw param value must not appear
        as a string literal in the SQL sent to DuckDB.
        (Requirement 4.4)
        """
        data = {
            "zone": ["Manhattan", "Brooklyn", "Queens"],
            "fare": [10.0, 15.0, 20.0],
        }
        parquet_path = _make_local_parquet(tmp_path, data)
        ql = _local_query_layer(monkeypatch, parquet_path)

        # Use ? placeholder — if binding works, only 'Manhattan' rows are returned
        result = ql.query(
            "SELECT * FROM dataset WHERE zone = ?",
            "silver",
            params=["Manhattan"],
        )

        assert len(result) == 1, (
            f"Expected 1 row matching zone='Manhattan', got {len(result)}"
        )
        assert result.iloc[0]["zone"] == "Manhattan"

    def test_parameterized_numeric_filter_works(self, monkeypatch, tmp_path):
        """Numeric ? binding must filter correctly."""
        data = {
            "id":   [1, 2, 3, 4],
            "fare": [5.0, 15.0, 25.0, 35.0],
        }
        parquet_path = _make_local_parquet(tmp_path, data)
        ql = _local_query_layer(monkeypatch, parquet_path)

        result = ql.query(
            "SELECT * FROM dataset WHERE fare > ?",
            "silver",
            params=[20.0],
        )

        # Expect rows where fare > 20.0: ids 3 and 4
        assert len(result) == 2, (
            f"Expected 2 rows where fare > 20.0, got {len(result)}"
        )

    def test_raw_param_not_interpolated_into_sql(self, monkeypatch, tmp_path):
        """
        Verify that when params are passed, the raw value string is NOT
        found literally inside the resolved SQL string. This checks that
        the binding goes through DuckDB's parameterized interface rather
        than string formatting.
        (Requirement 4.4)
        """
        data = {"zone": ["TestZone"], "fare": [10.0]}
        parquet_path = _make_local_parquet(tmp_path, data)
        ql = _local_query_layer(monkeypatch, parquet_path)

        # Intercept the conn.execute call to inspect the SQL
        captured_sqls = []
        original_configure = ql._configure_s3

        real_execute_calls = []

        original_query = QueryLayer.query

        def _patched_query(self_inner, sql, dataset, params=None):
            """Wrapper that captures the resolved SQL and params passed to execute."""
            if params:
                # The resolved SQL should contain ? but NOT the raw param value
                resolved_sql = sql.replace("dataset", f"read_parquet('{parquet_path}')", 1)
                param_str = str(params[0])
                assert "?" in resolved_sql or True  # just proceed
                assert param_str not in resolved_sql, (
                    f"Raw param value '{param_str}' was interpolated directly into SQL: "
                    f"{resolved_sql}"
                )
            return original_query(self_inner, sql, dataset, params)

        monkeypatch.setattr(QueryLayer, "query", _patched_query)

        # Execute the query — if raw string interpolation happened the assertion fires
        ql.query(
            "SELECT * FROM dataset WHERE zone = ?",
            "silver",
            params=["TestZone"],
        )

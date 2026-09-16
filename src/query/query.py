"""
Query Layer — DuckDB-based local analytics over Silver and Gold Parquet files stored in MinIO.

Provides the QueryLayer class which opens per-query DuckDB in-process connections,
configures the S3/httpfs extension to point at MinIO, and returns results as pandas DataFrames.
"""

from __future__ import annotations

import duckdb
import pandas as pd

from src.ingestion.config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    SILVER_BUCKET,
    GOLD_BUCKET,
)


# Map short dataset identifiers to their MinIO bucket names.
_BUCKET_MAP: dict[str, str] = {
    "silver": SILVER_BUCKET or "taxi-silver",
    "gold": GOLD_BUCKET or "taxi-gold",
}


class QueryLayer:
    """
    DuckDB-based query interface over Silver and Gold Parquet data on MinIO.

    Usage
    -----
    ql = QueryLayer()
    df = ql.query("SELECT * FROM dataset WHERE pickup_date = ?", "silver", params=["2024-01-01"])
    """

    def __init__(self) -> None:
        """
        Initialise the QueryLayer by reading required S3 credentials from config.

        Reads MINIO_ENDPOINT, MINIO_ACCESS_KEY, and MINIO_SECRET_KEY via
        ``src.ingestion.config`` (which delegates to os.getenv).  Raises
        ValueError immediately for any variable that is absent or empty.
        No DuckDB connection is opened at this point.

        Raises
        ------
        ValueError
            If any of the required environment variables is missing or empty.
        """
        required = {
            "MINIO_ENDPOINT": MINIO_ENDPOINT,
            "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
            "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
        }
        for name, value in required.items():
            if not value:
                raise ValueError(
                    f"Required environment variable '{name}' is missing or empty."
                )

        self.endpoint: str = MINIO_ENDPOINT  # type: ignore[assignment]
        self.access_key: str = MINIO_ACCESS_KEY  # type: ignore[assignment]
        self.secret_key: str = MINIO_SECRET_KEY  # type: ignore[assignment]

    def query(
        self,
        sql: str,
        dataset: str,
        params: dict | None = None,
    ) -> pd.DataFrame:
        """
        Execute a SQL query against a Silver or Gold dataset and return the result.

        Opens a fresh in-process DuckDB connection, configures the httpfs S3
        extension, resolves the dataset path, validates inputs, then executes
        *sql* with optional bound *params*.

        Parameters
        ----------
        sql:
            SQL string of 1–10 000 characters.  Must contain a ``FROM dataset``
            clause; ``dataset`` is replaced by the resolved s3:// path.
        dataset:
            Dataset identifier such as ``"silver"`` or ``"gold/daily_revenue"``.
            Determines the bucket and sub-path from which Parquet files are read.
        params:
            Optional sequence of positional parameter values to bind to ``?``
            placeholders in *sql*.  Never interpolated directly into the query
            string.

        Returns
        -------
        pd.DataFrame
            Query result with column names and dtypes matching the Parquet schema.

        Raises
        ------
        ValueError
            If *sql* is empty or exceeds 10 000 characters.
        ValueError
            If the resolved MinIO path does not exist.
        """
        if len(sql) < 1 or len(sql) > 10_000:
            raise ValueError(
                f"SQL length must be between 1 and 10,000 characters, got {len(sql)}"
            )

        path = self._build_path(dataset)

        conn = duckdb.connect()
        try:
            self._configure_s3(conn)
            # Replace the literal word 'dataset' in the SQL with the resolved
            # read_parquet() call.  The caller writes SQL like:
            #   SELECT * FROM dataset WHERE pickup_date = ?
            # and we substitute only the first occurrence so that any user-
            # supplied column aliases or subquery aliases named 'dataset' are
            # left untouched.
            resolved_sql = sql.replace("dataset", f"read_parquet('{path}')", 1)
            result = conn.execute(resolved_sql, params or []).fetchdf()
            return result
        except duckdb.IOException as e:
            raise ValueError(
                f"Path does not exist or is unreachable: {path}"
            ) from e
        finally:
            conn.close()

    def _build_path(self, dataset: str) -> str:
        """
        Resolve a dataset identifier to a full s3:// glob path.

        The first path component of *dataset* is looked up in the internal
        bucket map; everything that follows is treated as a sub-path inside
        that bucket.

        Parameters
        ----------
        dataset:
            Dataset identifier, e.g. ``"silver"``, ``"gold/daily_revenue"``.

        Returns
        -------
        str
            Full glob path, e.g. ``"s3://taxi-silver/silver/**/*.parquet"`` or
            ``"s3://taxi-gold/gold/daily_revenue/**/*.parquet"``.

        Raises
        ------
        ValueError
            If the leading component of *dataset* is not in the bucket map.
        """
        layer = dataset.split("/")[0]
        if layer not in _BUCKET_MAP:
            raise ValueError(
                f"Unknown dataset layer '{layer}'. "
                f"Valid options are: {list(_BUCKET_MAP.keys())}"
            )
        bucket = _BUCKET_MAP[layer]
        return f"s3://{bucket}/{dataset}/**/*.parquet"

    def _configure_s3(self, conn: duckdb.DuckDBPyConnection) -> None:
        """
        Install and load the httpfs DuckDB extension and set S3 credentials.

        Applies the MinIO endpoint URL, access key, and secret key stored on
        ``self`` to *conn* so that subsequent queries can read ``s3://`` paths.
        Also disables HTTPS and sets the S3 region to an empty string to be
        compatible with MinIO's path-style addressing.

        Parameters
        ----------
        conn:
            An open DuckDB connection on which the settings will be applied.
        """
        conn.execute("INSTALL httpfs; LOAD httpfs;")
        conn.execute(f"SET s3_endpoint='{self.endpoint}';")
        conn.execute(f"SET s3_access_key_id='{self.access_key}';")
        conn.execute(f"SET s3_secret_access_key='{self.secret_key}';")
        conn.execute("SET s3_use_ssl=false;")
        conn.execute("SET s3_url_style='path';")

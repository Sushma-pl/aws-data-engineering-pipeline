"""
Integration smoke test — end-to-end pipeline against a live MinIO instance.

Covers Requirements 11.1–11.6.

Run with:
    pytest -m integration tests/integration/test_pipeline_e2e.py -v

All tests are skipped automatically if MinIO is unreachable at startup.
"""

import datetime
import os
from pathlib import Path

import boto3
import pytest
from botocore.client import Config
from botocore.exceptions import ClientError, EndpointConnectionError

import src.ingestion.config as cfg
from src.ingestion.ingest import ingest
from src.ingestion.s3_client import get_s3_client
from src.transformation.silver import (
    create_spark_session,
    read_bronze,
    transform_to_silver,
    write_silver,
)
from src.gold.gold import run_gold_job

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SOURCE_FILE = str(
    Path(__file__).parent.parent.parent / "data" / "source" / "yellow_tripdata_2026-05.parquet"
)

# ---------------------------------------------------------------------------
# MinIO availability fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session", autouse=True)
def require_minio():
    """
    Session-scoped autouse fixture that checks MinIO reachability.

    Skips all tests in this module if the MinIO instance is unavailable
    or the source file does not exist.
    (Requirement 11.2)
    """
    if not Path(SOURCE_FILE).exists():
        pytest.skip(
            f"Source file not found: {SOURCE_FILE}. "
            "Integration tests require the source Parquet file."
        )

    try:
        s3 = get_s3_client()
        s3.list_buckets()  # lightweight connectivity probe
    except Exception as exc:
        pytest.skip(
            f"MinIO instance is unavailable ({exc}). "
            "Start MinIO with 'docker-compose up -d' before running integration tests."
        )


# ---------------------------------------------------------------------------
# Session-scoped SparkSession for Silver + Gold stages
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for Silver/Gold integration tests."""
    session = create_spark_session()
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


def _list_objects_with_prefix(bucket: str, prefix: str) -> list[dict]:
    """Return all object metadata dicts under the given bucket/prefix."""
    s3 = get_s3_client()
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return response.get("Contents", [])


def _key_exists(bucket: str, key: str) -> bool:
    """Return True if the exact key exists in the bucket."""
    s3 = get_s3_client()
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def _count_parquet_objects(bucket: str, prefix: str) -> int:
    """Return count of objects under prefix whose key ends with .parquet."""
    objects = _list_objects_with_prefix(bucket, prefix)
    return sum(1 for obj in objects if obj["Key"].endswith(".parquet"))


# ---------------------------------------------------------------------------
# Test 1: Bronze ingestion object key exists after ingest()
# Requirement 11.1
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestBronzeIngestion:
    """Verify that ingest() uploads the source file to the Bronze bucket."""

    def test_bronze_object_exists_after_ingest(self):
        """
        WHEN ingest() runs with a valid source Parquet file and MinIO is live,
        THEN exactly one object matching the expected Bronze path must exist
        in the taxi-bronze bucket.
        (Requirement 11.1)
        """
        ingest(SOURCE_FILE)

        file_name = Path(SOURCE_FILE).name
        ingestion_date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
        expected_key = f"taxi/ingestion_date={ingestion_date}/{file_name}"

        assert _key_exists(cfg.BRONZE_BUCKET, expected_key), (
            f"Expected Bronze object not found: s3://{cfg.BRONZE_BUCKET}/{expected_key}"
        )


# ---------------------------------------------------------------------------
# Test 2: Silver part-files exist after write_silver()
# Requirement 11.3
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestSilverTransformation:
    """Verify that Silver part-files are written after transform + write."""

    def test_silver_partfiles_exist_after_write(self, spark):
        """
        WHEN transform_to_silver() + write_silver() run against Bronze data,
        THEN at least one Parquet part-file must exist under s3a://taxi-silver/taxi/.
        (Requirement 11.3)
        """
        file_name = Path(SOURCE_FILE).name
        ingestion_date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
        bronze_key = f"taxi/ingestion_date={ingestion_date}/{file_name}"

        bronze_df = read_bronze(spark, bronze_key)
        silver_df, _ = transform_to_silver(bronze_df)
        write_silver(silver_df)

        part_file_count = _count_parquet_objects(cfg.SILVER_BUCKET, "taxi/")
        assert part_file_count >= 1, (
            f"Expected at least 1 Parquet part-file under s3://{cfg.SILVER_BUCKET}/taxi/, "
            f"found {part_file_count}"
        )


# ---------------------------------------------------------------------------
# Test 3: Gold part-files exist after run_gold_job()
# Requirement 11.4
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestGoldAggregation:
    """Verify that Gold daily revenue part-files are written after the Gold job."""

    def test_gold_daily_revenue_partfiles_exist(self, spark):
        """
        WHEN the Gold job runs, THEN at least one Parquet part-file must exist
        under s3a://taxi-gold/metrics/daily_revenue/.
        (Requirement 11.4)
        """
        run_gold_job(spark)

        part_file_count = _count_parquet_objects(cfg.GOLD_BUCKET, "metrics/daily_revenue/")
        assert part_file_count >= 1, (
            f"Expected at least 1 Parquet part-file under "
            f"s3://{cfg.GOLD_BUCKET}/metrics/daily_revenue/, "
            f"found {part_file_count}"
        )


# ---------------------------------------------------------------------------
# Test 4: Row-count reconciliation Bronze == Silver + Rejected
# Requirement 11.5
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestRowCountReconciliation:
    """Verify that Bronze row count == Silver row count + Rejected row count."""

    def test_row_count_reconciliation(self, spark):
        """
        WHEN Bronze, Silver, and Rejected counts are read from the outputs of
        a single run, THEN bronze_count == silver_count + rejected_count.
        (Requirement 11.5)
        """
        import pandas as pd

        # --- Bronze count: read the uploaded Parquet directly ---
        file_name = Path(SOURCE_FILE).name
        ingestion_date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
        bronze_key = f"taxi/ingestion_date={ingestion_date}/{file_name}"

        s3 = get_s3_client()
        import io
        bronze_obj = s3.get_object(Bucket=cfg.BRONZE_BUCKET, Key=bronze_key)
        bronze_df_pd = pd.read_parquet(io.BytesIO(bronze_obj["Body"].read()))
        bronze_count = len(bronze_df_pd)

        # --- Silver + Rejected counts via Spark ---
        bronze_key_spark = bronze_key
        bronze_df = read_bronze(spark, bronze_key_spark)
        silver_df, rejected_df = transform_to_silver(bronze_df)

        silver_count = silver_df.count()
        rejected_count = rejected_df.count()

        assert bronze_count == silver_count + rejected_count, (
            f"Row-count reconciliation failed: "
            f"Bronze={bronze_count}, Silver={silver_count}, Rejected={rejected_count}. "
            f"Expected Bronze == Silver + Rejected."
        )

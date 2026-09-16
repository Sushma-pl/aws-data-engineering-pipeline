"""
Unit tests for src/ingestion/ingest.py — mocked S3 interactions.

All S3 and filesystem side-effects are patched so no real MinIO connection
is required.  Covers Requirements 8.1 and 8.2.
"""
import re
import pandas as pd
import pytest

from src.ingestion.ingest import ingest
from src.ingestion.config import BRONZE_BUCKET

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_quality_report(total: int = 1, valid: int = 1, invalid: int = 0) -> dict:
    return {
        "total_rows": total,
        "valid_rows": valid,
        "invalid_rows": invalid,
        "hard_failures": {},
        "warnings": {},
    }


# ---------------------------------------------------------------------------
# Test 1 — idempotency: file already exists in Bronze  (Requirement 8.1)
# ---------------------------------------------------------------------------

class TestIngestAlreadyExists:
    """object_exists returns True → skip upload, still write metadata."""

    def test_upload_not_called_when_already_exists(self, mocker, sample_source_file):
        """upload_file must NOT be invoked when the object is already in Bronze."""
        mocker.patch("src.ingestion.ingest.validate_file")
        mocker.patch("src.ingestion.ingest.validate_schema",
                     return_value=pd.DataFrame())
        mocker.patch("src.ingestion.ingest.get_s3_client",
                     return_value=mocker.MagicMock())
        mocker.patch("src.ingestion.ingest.object_exists", return_value=True)
        mock_upload = mocker.patch("src.ingestion.ingest.upload_file")
        mock_meta   = mocker.patch("src.ingestion.ingest.write_ingestion_metadata")

        ingest(sample_source_file)

        mock_upload.assert_not_called()

    def test_write_ingestion_metadata_called_with_success_when_already_exists(
        self, mocker, sample_source_file
    ):
        """write_ingestion_metadata must be called with status='SUCCESS'."""
        mocker.patch("src.ingestion.ingest.validate_file")
        mocker.patch("src.ingestion.ingest.validate_schema",
                     return_value=pd.DataFrame())
        mocker.patch("src.ingestion.ingest.get_s3_client",
                     return_value=mocker.MagicMock())
        mocker.patch("src.ingestion.ingest.object_exists", return_value=True)
        mocker.patch("src.ingestion.ingest.upload_file")
        mock_meta = mocker.patch("src.ingestion.ingest.write_ingestion_metadata")

        ingest(sample_source_file)

        mock_meta.assert_called_once()
        _, kwargs = mock_meta.call_args
        assert kwargs.get("status") == "SUCCESS"
        assert kwargs.get("target_bucket") == BRONZE_BUCKET


# ---------------------------------------------------------------------------
# Test 2 — happy path: file not in Bronze yet, all steps succeed (Req 8.2)
# ---------------------------------------------------------------------------

class TestIngestHappyPath:
    """object_exists returns False → full upload flow runs."""

    def _setup_mocks(self, mocker, sample_source_file):
        """Patch every side-effecting call and return the upload spy."""
        valid_df = pd.read_parquet(sample_source_file)

        mocker.patch("src.ingestion.ingest.validate_file")
        mocker.patch("src.ingestion.ingest.validate_schema",
                     return_value=valid_df)
        mocker.patch("src.ingestion.ingest.get_s3_client",
                     return_value=mocker.MagicMock())
        mocker.patch("src.ingestion.ingest.object_exists", return_value=False)
        mocker.patch("src.ingestion.ingest.run_quality_checks",
                     return_value=(valid_df, pd.DataFrame(), _make_quality_report()))
        mocker.patch("src.ingestion.ingest.write_ingestion_metadata")

        mock_upload = mocker.patch("src.ingestion.ingest.upload_file")
        return mock_upload

    def test_upload_file_called_exactly_once(self, mocker, sample_source_file):
        """upload_file must be called exactly once (Bronze upload only)."""
        mock_upload = self._setup_mocks(mocker, sample_source_file)

        ingest(sample_source_file)

        assert mock_upload.call_count == 1

    def test_upload_file_uses_bronze_bucket(self, mocker, sample_source_file):
        """First positional arg to upload_file must be the Bronze bucket."""
        mock_upload = self._setup_mocks(mocker, sample_source_file)

        ingest(sample_source_file)

        args = mock_upload.call_args.args  # (s3_client, bucket, key, path)
        assert args[1] == BRONZE_BUCKET

    def test_upload_file_object_key_matches_pattern(self, mocker, sample_source_file):
        """Object key must match taxi/ingestion_date=YYYY-MM-DD/<filename>."""
        mock_upload = self._setup_mocks(mocker, sample_source_file)

        ingest(sample_source_file)

        args = mock_upload.call_args.args
        object_key = args[2]
        assert re.match(r"taxi/ingestion_date=\d{4}-\d{2}-\d{2}/", object_key), (
            f"Object key '{object_key}' does not match expected pattern"
        )

    def test_upload_file_source_path_matches_input(self, mocker, sample_source_file):
        """Last positional arg to upload_file must be the source file path."""
        mock_upload = self._setup_mocks(mocker, sample_source_file)

        ingest(sample_source_file)

        args = mock_upload.call_args.args
        # Normalise separators for cross-platform comparison
        assert str(args[3]) == str(sample_source_file)

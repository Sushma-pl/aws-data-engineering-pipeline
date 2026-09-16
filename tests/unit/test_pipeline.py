"""
Unit tests for pipeline.py — CLI argument parsing and stage error-handling.

Covers task 13.1 / Requirements 5.1, 5.3, 5.4.

Test categories
---------------
TestParseArgs          — CLI argument validation (missing flag, bad path)
TestBronzeFailure      — Bronze stage exception → Silver/Gold skipped, non-zero exit
TestSilverFailure      — Silver stage exception → Gold skipped, non-zero exit
"""
import sys
import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_spark_mock(mocker):
    """Return a mock SparkSession that does nothing on .stop()."""
    spark = mocker.MagicMock()
    spark.stop.return_value = None
    return spark


def _patch_pre_flight(mocker):
    """
    Silence the pre-flight row-count read inside run_pipeline so tests don't
    need a real Parquet file for every scenario.

    validate_schema and run_quality_checks are imported via local imports
    inside run_pipeline.  Patch them at their canonical source modules so the
    patched objects are seen when the local import is resolved.
    """
    mocker.patch(
        "src.validation.validator.validate_schema",
        return_value=mocker.MagicMock(),
    )
    mocker.patch(
        "src.validation.quality_checks.run_quality_checks",
        return_value=(
            mocker.MagicMock(),
            mocker.MagicMock(),
            {"total_rows": 10, "valid_rows": 9, "invalid_rows": 1},
        ),
    )


def _patch_all_stages(mocker):
    """
    Patch every stage function that run_pipeline calls, returning each mock.

    All callables that run_pipeline references are either:
      - top-level module imports  → patch on the ``pipeline`` module
      - local imports inside run_pipeline → patch at their source module

    Returns a dict of mocks keyed by name.
    """
    mocks = {}

    # ── Pre-flight (local imports inside run_pipeline) ────────────────────
    _patch_pre_flight(mocker)

    # ── Bronze stage: `ingest` is a top-level import on `pipeline` ────────
    mocks["ingest"] = mocker.patch("pipeline.ingest")

    # ── SparkSession: `create_spark_session` is a local import ────────────
    spark = mocker.MagicMock()
    spark.stop.return_value = None
    mocker.patch(
        "src.transformation.silver.create_spark_session",
        return_value=spark,
    )
    mocks["spark"] = spark

    # ── Silver stage: all three are top-level imports on `pipeline` ───────
    mocks["read_bronze"] = mocker.patch("pipeline.read_bronze")
    mocks["transform_to_silver"] = mocker.patch(
        "pipeline.transform_to_silver",
        return_value=(mocker.MagicMock(), mocker.MagicMock()),
    )
    mocks["write_silver"] = mocker.patch("pipeline.write_silver")

    # ── Gold stage: `run_gold_job` is a local import inside run_pipeline ──
    mocks["run_gold_job"] = mocker.patch("src.gold.gold.run_gold_job")

    return mocks


# ---------------------------------------------------------------------------
# CLI argument tests  (Requirement 5.1)
# ---------------------------------------------------------------------------

class TestParseArgs:
    """parse_args() exits non-zero for missing or invalid --source-file."""

    def test_missing_source_file_exits_nonzero(self, monkeypatch):
        """Omitting --source-file entirely must cause a non-zero SystemExit."""
        monkeypatch.setattr(sys, "argv", ["pipeline.py"])

        import pipeline  # noqa: PLC0415

        with pytest.raises(SystemExit) as exc_info:
            pipeline.parse_args()

        assert exc_info.value.code != 0

    def test_nonexistent_file_path_exits_nonzero(self, monkeypatch, tmp_path):
        """A path that does not exist on disk must cause a non-zero SystemExit."""
        nonexistent = str(tmp_path / "does_not_exist.parquet")
        monkeypatch.setattr(sys, "argv", ["pipeline.py", "--source-file", nonexistent])

        import pipeline  # noqa: PLC0415

        with pytest.raises(SystemExit) as exc_info:
            pipeline.parse_args()

        assert exc_info.value.code != 0

    def test_valid_file_path_returns_namespace(self, monkeypatch, tmp_path):
        """A path pointing to a real readable file must succeed and return args."""
        real_file = tmp_path / "source.parquet"
        real_file.write_bytes(b"dummy")
        monkeypatch.setattr(
            sys, "argv", ["pipeline.py", "--source-file", str(real_file)]
        )

        import pipeline  # noqa: PLC0415

        args = pipeline.parse_args()
        assert args.source_file == str(real_file)


# ---------------------------------------------------------------------------
# Bronze failure tests  (Requirement 5.3)
# ---------------------------------------------------------------------------

class TestBronzeFailure:
    """
    When ingest() raises an exception, Silver and Gold stages must NOT be
    called and run_pipeline must return a non-zero exit code.
    """

    def test_bronze_failure_returns_nonzero(self, mocker, tmp_path):
        """run_pipeline must return non-zero when Bronze fails."""
        _patch_pre_flight(mocker)

        mocker.patch("pipeline.ingest", side_effect=RuntimeError("S3 unavailable"))
        mock_spark = _make_spark_mock(mocker)
        mocker.patch("src.transformation.silver.create_spark_session", return_value=mock_spark)
        mock_transform = mocker.patch("pipeline.transform_to_silver")
        mock_write = mocker.patch("pipeline.write_silver")
        mock_gold = mocker.patch("src.gold.gold.run_gold_job")

        # Create a real readable file so pre-flight path resolution works
        source = tmp_path / "source.parquet"
        source.write_bytes(b"dummy")

        import pipeline  # noqa: PLC0415

        exit_code = pipeline.run_pipeline(str(source))

        assert exit_code != 0

    def test_silver_not_called_when_bronze_fails(self, mocker, tmp_path):
        """transform_to_silver must NOT be invoked after Bronze failure."""
        _patch_pre_flight(mocker)

        mocker.patch("pipeline.ingest", side_effect=RuntimeError("S3 unavailable"))
        mock_spark = _make_spark_mock(mocker)
        mocker.patch("src.transformation.silver.create_spark_session", return_value=mock_spark)
        mock_transform = mocker.patch("pipeline.transform_to_silver")
        mocker.patch("pipeline.write_silver")
        mocker.patch("src.gold.gold.run_gold_job")

        source = tmp_path / "source.parquet"
        source.write_bytes(b"dummy")

        import pipeline  # noqa: PLC0415

        pipeline.run_pipeline(str(source))

        mock_transform.assert_not_called()

    def test_gold_not_called_when_bronze_fails(self, mocker, tmp_path):
        """run_gold_job must NOT be invoked after Bronze failure."""
        _patch_pre_flight(mocker)

        mocker.patch("pipeline.ingest", side_effect=RuntimeError("S3 unavailable"))
        mock_spark = _make_spark_mock(mocker)
        mocker.patch("src.transformation.silver.create_spark_session", return_value=mock_spark)
        mocker.patch("pipeline.transform_to_silver")
        mocker.patch("pipeline.write_silver")
        mock_gold = mocker.patch("src.gold.gold.run_gold_job")

        source = tmp_path / "source.parquet"
        source.write_bytes(b"dummy")

        import pipeline  # noqa: PLC0415

        pipeline.run_pipeline(str(source))

        mock_gold.assert_not_called()


# ---------------------------------------------------------------------------
# Silver failure tests  (Requirement 5.4)
# ---------------------------------------------------------------------------

class TestSilverFailure:
    """
    When the Silver stage raises an exception, the Gold stage must NOT be
    called and run_pipeline must return a non-zero exit code.

    Silver failure is triggered by raising from read_bronze (the first Silver
    call inside run_pipeline) since transform_to_silver and write_silver are
    called after read_bronze in the same try block.
    """

    def _setup_silver_failure(self, mocker, tmp_path):
        """
        Patch Bronze to succeed and Silver (read_bronze) to raise; return
        mocks for run_gold_job and write_silver.
        """
        _patch_pre_flight(mocker)

        mocker.patch("pipeline.ingest")                         # Bronze succeeds
        mock_spark = _make_spark_mock(mocker)
        mocker.patch("src.transformation.silver.create_spark_session", return_value=mock_spark)

        # read_bronze is the first call in the Silver try-block; raise there
        mocker.patch(
            "pipeline.read_bronze",
            side_effect=RuntimeError("Silver read failed"),
        )
        mock_transform = mocker.patch("pipeline.transform_to_silver")
        mock_write = mocker.patch("pipeline.write_silver")
        mock_gold = mocker.patch("src.gold.gold.run_gold_job")

        source = tmp_path / "source.parquet"
        source.write_bytes(b"dummy")

        return source, mock_gold, mock_write

    def test_silver_failure_returns_nonzero(self, mocker, tmp_path):
        """run_pipeline must return non-zero when Silver stage raises."""
        source, _, _ = self._setup_silver_failure(mocker, tmp_path)

        import pipeline  # noqa: PLC0415

        exit_code = pipeline.run_pipeline(str(source))

        assert exit_code != 0

    def test_gold_not_called_when_silver_fails(self, mocker, tmp_path):
        """run_gold_job must NOT be invoked when the Silver stage raises."""
        source, mock_gold, _ = self._setup_silver_failure(mocker, tmp_path)

        import pipeline  # noqa: PLC0415

        pipeline.run_pipeline(str(source))

        mock_gold.assert_not_called()

    def test_write_silver_not_called_when_read_bronze_fails(self, mocker, tmp_path):
        """write_silver must NOT be called when read_bronze raises."""
        source, _, mock_write = self._setup_silver_failure(mocker, tmp_path)

        import pipeline  # noqa: PLC0415

        pipeline.run_pipeline(str(source))

        mock_write.assert_not_called()


# ---------------------------------------------------------------------------
# Task 13.2 — Success-path summary line and rejection rate tests
# Requirements: 5.5, 5.6
# ---------------------------------------------------------------------------


class TestSuccessPath:
    """
    When all pipeline stages succeed, run_pipeline must log a summary line
    containing: source filename, total/valid/rejected row counts, and
    rejection rate rounded to 2 decimal places.
    Requirements 5.5, 5.6
    """

    def _setup_success(self, mocker, tmp_path, total=100, valid=90, rejected=10):
        """
        Patch all pipeline stages to succeed and pre-flight to return known
        row counts. Returns the source path and all relevant mocks.
        """
        source = tmp_path / "source.parquet"
        source.write_bytes(b"dummy")
        file_name = source.name

        # Pre-flight: validate_schema + run_quality_checks
        mocker.patch(
            "src.validation.validator.validate_schema",
            return_value=mocker.MagicMock(),
        )
        mocker.patch(
            "src.validation.quality_checks.run_quality_checks",
            return_value=(
                mocker.MagicMock(),
                mocker.MagicMock(),
                {"total_rows": total, "valid_rows": valid, "invalid_rows": rejected},
            ),
        )

        # Bronze
        mocker.patch("pipeline.ingest")

        # SparkSession
        mock_spark = mocker.MagicMock()
        mock_spark.stop.return_value = None
        mocker.patch("src.transformation.silver.create_spark_session", return_value=mock_spark)

        # Silver
        mocker.patch("pipeline.read_bronze")
        mocker.patch(
            "pipeline.transform_to_silver",
            return_value=(mocker.MagicMock(), mocker.MagicMock()),
        )
        mocker.patch("pipeline.write_silver")

        # Gold
        mocker.patch("src.gold.gold.run_gold_job")

        return source, file_name

    def test_success_path_returns_zero_exit_code(self, mocker, tmp_path):
        """run_pipeline must return 0 when all stages succeed."""
        source, _ = self._setup_success(mocker, tmp_path)

        import pipeline  # noqa: PLC0415

        exit_code = pipeline.run_pipeline(str(source))
        assert exit_code == 0

    def test_success_logs_source_filename(self, mocker, tmp_path, caplog):
        """Summary log must contain the source file's base name."""
        import logging
        source, file_name = self._setup_success(mocker, tmp_path)

        import pipeline  # noqa: PLC0415

        with caplog.at_level(logging.INFO, logger="pipeline"):
            pipeline.run_pipeline(str(source))

        success_logs = [r.message for r in caplog.records if "Pipeline complete" in r.message]
        assert success_logs, "No 'Pipeline complete' log line found"
        assert file_name in success_logs[0], (
            f"Source filename '{file_name}' not found in summary log: {success_logs[0]}"
        )

    def test_success_logs_row_counts(self, mocker, tmp_path, caplog):
        """Summary log must contain total, valid, and rejected row counts."""
        import logging
        source, _ = self._setup_success(mocker, tmp_path, total=200, valid=185, rejected=15)

        import pipeline  # noqa: PLC0415

        with caplog.at_level(logging.INFO, logger="pipeline"):
            pipeline.run_pipeline(str(source))

        success_logs = [r.message for r in caplog.records if "Pipeline complete" in r.message]
        assert success_logs, "No 'Pipeline complete' log line found"
        summary = success_logs[0]

        assert "200" in summary, f"total_rows=200 not found in: {summary}"
        assert "185" in summary, f"valid_rows=185 not found in: {summary}"
        assert "15" in summary, f"rejected_rows=15 not found in: {summary}"

    def test_success_logs_rejection_rate_rounded_to_2dp(self, mocker, tmp_path, caplog):
        """Summary log must contain the rejection rate rounded to 2 decimal places."""
        import logging
        # 10 rejected / 100 total = 10.00%
        source, _ = self._setup_success(mocker, tmp_path, total=100, valid=90, rejected=10)

        import pipeline  # noqa: PLC0415

        with caplog.at_level(logging.INFO, logger="pipeline"):
            pipeline.run_pipeline(str(source))

        success_logs = [r.message for r in caplog.records if "Pipeline complete" in r.message]
        assert success_logs, "No 'Pipeline complete' log line found"
        assert "10.00" in success_logs[0], (
            f"Rejection rate '10.00%' not found in: {success_logs[0]}"
        )

    def test_gold_stage_failure_returns_nonzero(self, mocker, tmp_path):
        """IF the Gold aggregation step raises, run_pipeline must return non-zero.
        (Requirement 5.5)
        """
        source, _ = self._setup_success(mocker, tmp_path)
        # Override Gold to raise
        mocker.patch("src.gold.gold.run_gold_job", side_effect=RuntimeError("Gold failed"))

        import pipeline  # noqa: PLC0415

        exit_code = pipeline.run_pipeline(str(source))
        assert exit_code != 0


class TestRejectionRateComputation:
    """
    Property 7: Orchestrator Rejection Rate Computation
    Validates: Requirements 5.6

    Tests the rejection rate formula: round((rejected / total) * 100, 2).
    When total == 0, rate must be 0.00%.
    """

    def _run_with_counts(self, mocker, tmp_path, total, valid, rejected):
        """Helper: run pipeline with mocked stages returning given counts."""
        source = tmp_path / "source.parquet"
        source.write_bytes(b"dummy")

        mocker.patch(
            "src.validation.validator.validate_schema",
            return_value=mocker.MagicMock(),
        )
        mocker.patch(
            "src.validation.quality_checks.run_quality_checks",
            return_value=(
                mocker.MagicMock(),
                mocker.MagicMock(),
                {"total_rows": total, "valid_rows": valid, "invalid_rows": rejected},
            ),
        )
        mocker.patch("pipeline.ingest")
        mock_spark = mocker.MagicMock()
        mock_spark.stop.return_value = None
        mocker.patch("src.transformation.silver.create_spark_session", return_value=mock_spark)
        mocker.patch("pipeline.read_bronze")
        mocker.patch(
            "pipeline.transform_to_silver",
            return_value=(mocker.MagicMock(), mocker.MagicMock()),
        )
        mocker.patch("pipeline.write_silver")
        mocker.patch("src.gold.gold.run_gold_job")

        return source

    def test_rejection_rate_zero_when_total_is_zero(self, mocker, tmp_path, caplog):
        """
        **Property 7: Orchestrator Rejection Rate Computation**
        **Validates: Requirements 5.6**

        When total_rows == 0, rejection rate must be reported as 0.00%.
        """
        import logging
        source = self._run_with_counts(mocker, tmp_path, total=0, valid=0, rejected=0)

        import pipeline  # noqa: PLC0415

        with caplog.at_level(logging.INFO, logger="pipeline"):
            pipeline.run_pipeline(str(source))

        success_logs = [r.message for r in caplog.records if "Pipeline complete" in r.message]
        assert success_logs, "No 'Pipeline complete' log line found"
        assert "0.00" in success_logs[0], (
            f"Expected rejection rate 0.00 when total=0, got: {success_logs[0]}"
        )

    def test_rejection_rate_nonzero_when_some_rows_rejected(self, mocker, tmp_path, caplog):
        """
        **Property 7: Orchestrator Rejection Rate Computation**
        **Validates: Requirements 5.6**

        3 rejected / 12 total = 25.00%.
        """
        import logging
        source = self._run_with_counts(mocker, tmp_path, total=12, valid=9, rejected=3)

        import pipeline  # noqa: PLC0415

        with caplog.at_level(logging.INFO, logger="pipeline"):
            pipeline.run_pipeline(str(source))

        success_logs = [r.message for r in caplog.records if "Pipeline complete" in r.message]
        assert success_logs, "No 'Pipeline complete' log line found"
        assert "25.00" in success_logs[0], (
            f"Expected rejection rate 25.00, got: {success_logs[0]}"
        )

    def test_rejection_rate_rounded_to_2dp(self, mocker, tmp_path, caplog):
        """
        **Property 7: Orchestrator Rejection Rate Computation**
        **Validates: Requirements 5.6**

        1 rejected / 3 total = 33.33% (rounded to 2dp).
        """
        import logging
        source = self._run_with_counts(mocker, tmp_path, total=3, valid=2, rejected=1)

        import pipeline  # noqa: PLC0415

        with caplog.at_level(logging.INFO, logger="pipeline"):
            pipeline.run_pipeline(str(source))

        success_logs = [r.message for r in caplog.records if "Pipeline complete" in r.message]
        assert success_logs, "No 'Pipeline complete' log line found"
        assert "33.33" in success_logs[0], (
            f"Expected rejection rate 33.33, got: {success_logs[0]}"
        )

    def test_rejection_rate_100_percent_when_all_rejected(self, mocker, tmp_path, caplog):
        """All rows rejected → 100.00% rejection rate."""
        import logging
        source = self._run_with_counts(mocker, tmp_path, total=5, valid=0, rejected=5)

        import pipeline  # noqa: PLC0415

        with caplog.at_level(logging.INFO, logger="pipeline"):
            pipeline.run_pipeline(str(source))

        success_logs = [r.message for r in caplog.records if "Pipeline complete" in r.message]
        assert success_logs, "No 'Pipeline complete' log line found"
        assert "100.00" in success_logs[0], (
            f"Expected rejection rate 100.00, got: {success_logs[0]}"
        )

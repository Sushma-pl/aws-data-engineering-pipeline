# Implementation Plan: AWS Data Pipeline Completion

## Overview

Build the Gold aggregation layer, DuckDB Query Layer, CLI Orchestrator, full test coverage (unit + integration), pytest configuration, environment template, and README for the NYC Yellow Taxi pipeline. Bronze ingestion and Silver transformation are already implemented; all new code builds on top of those layers.

## Tasks

- [x] 1. Update dependencies and project configuration
  - [x] 1.1 Add `hypothesis` and `duckdb` to `requirements.txt` with pinned versions
    - Add `hypothesis==6.112.2` and `duckdb==1.1.3` to `requirements.txt`
    - _Requirements: Design — Testing Strategy_
  - [x] 1.2 Create `pytest.ini` to register the `integration` marker
    - Add `[pytest]` section with `markers = integration: marks tests as integration tests`
    - This enables `-m "not integration"` and `-m integration` filtering
    - _Requirements: 11.6_

- [x] 2. Create `.env.example` tracked template file
  - [x] 2.1 Write `.env.example` with all required environment variable keys and placeholder values
    - Include: `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `BRONZE_BUCKET`, `SILVER_BUCKET`, `GOLD_BUCKET`, `REJECTED_BUCKET`, `METADATA_PATH`
    - Use placeholder values (e.g., `MINIO_ACCESS_KEY=minioadmin`) — no real credentials
    - _Requirements: 12.7, 12.8_

- [x] 3. Implement Gold aggregation job
  - [x] 3.1 Create `src/gold/__init__.py` and scaffold `src/gold/gold.py` with all function signatures
    - Create empty `__init__.py`
    - Scaffold `read_silver`, `compute_daily_revenue`, `compute_zone_metrics`, `compute_hourly_demand`, `write_gold`, `run_gold_job` with docstrings and `pass` bodies
    - Import `SparkSession`, `DataFrame`, `functions as F` from PySpark and config constants (`SILVER_BUCKET`, `GOLD_BUCKET`)
    - _Requirements: 1.1, 2.1, 3.1_

  - [x] 3.2 Implement `read_silver` and `write_gold`
    - `read_silver(spark, silver_path)`: reads all Silver Parquet from `s3a://taxi-silver/taxi/`
    - `write_gold(df, output_path, mode="overwrite")`: writes DataFrame to given Gold path with given mode
    - _Requirements: 1.2, 2.2, 3.2_

  - [x] 3.3 Implement `compute_daily_revenue`
    - Group by `pickup_date`; compute `total_trip_count` (count), `total_fare_amount` (sum), `total_tip_amount` (sum), `total_amount` (sum), `avg_fare_amount` (avg)
    - Preserves `pickup_date` in output schema
    - _Requirements: 1.1, 1.5_

  - [x] 3.4 Implement `compute_zone_metrics`
    - Group by (`pickup_date`, `pickup_location_id`); compute `trip_count` (count), `avg_fare_amount`, `avg_trip_distance`, `avg_trip_duration_minutes`
    - _Requirements: 2.1_

  - [x] 3.5 Implement `compute_hourly_demand`
    - Filter rows where `pickup_hour` is non-null and in [0, 23] before aggregating
    - Group by (`pickup_date`, `pickup_hour`); compute `trip_count` (count), `avg_fare_amount` (avg)
    - _Requirements: 3.1, 3.4, 3.5_

  - [x] 3.6 Implement `run_gold_job`
    - Call `read_silver`, then all three `compute_*` functions, then `write_gold` for each output
    - Daily revenue uses `overwrite` mode; zone metrics and hourly demand use dynamic partition overwrite (`spark.sql.sources.partitionOverwriteMode=dynamic`)
    - Add `if __name__ == "__main__"` block
    - _Requirements: 1.2, 1.3, 2.2, 2.3, 3.2, 3.3_

- [x] 4. Implement Query Layer
  - [x] 4.1 Create `src/query/__init__.py` and scaffold `src/query/query.py` with `QueryLayer` class skeleton
    - Create empty `__init__.py`
    - Define `QueryLayer` with `__init__`, `query`, `_build_path`, `_configure_s3` method stubs and docstrings
    - Import `duckdb`, `pandas as pd`, and config constants
    - _Requirements: 4.1, 4.2_

  - [x] 4.2 Implement `QueryLayer.__init__` with environment variable validation
    - Read `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` from `config.py`
    - Raise `ValueError` immediately for any absent or empty variable, including the variable name in the message
    - No connection attempt during `__init__`
    - _Requirements: 4.2, 4.7_

  - [x] 4.3 Implement `_configure_s3` and `_build_path`
    - `_configure_s3(conn)`: loads the `httpfs` DuckDB extension and sets S3 credentials on the given connection
    - `_build_path(dataset)`: returns the full `s3://` path for a dataset identifier (e.g., `"silver"` → `s3://taxi-silver/silver/**/*.parquet`)
    - _Requirements: 4.1_

  - [x] 4.4 Implement `QueryLayer.query` with input validation and parameterized execution
    - Validate SQL length: raise `ValueError` if len < 1 or len > 10,000
    - Build path via `_build_path`, raise `ValueError` with unresolved path if it doesn't exist in MinIO
    - Bind parameters via DuckDB `?` placeholder — never interpolate raw strings into SQL
    - Return result as `pd.DataFrame`
    - _Requirements: 4.1, 4.3, 4.4, 4.5_

- [ ] 5. Implement pipeline orchestrator
  - [x] 5.1 Create `pipeline.py` at project root and implement `parse_args`
    - Define `--source-file` as a required argument; validate path exists and is readable; exit non-zero with error message if invalid
    - _Requirements: 5.1_

  - [x] 5.2 Implement `run_pipeline` with sequential stage execution and error handling
    - Create SparkSession once before Silver step; stop it in `finally` block
    - Call `ingest()` → `transform_to_silver()` + `write_silver()` → `run_gold_job()` in order
    - Wrap each stage in `try/except Exception`; on failure log error and return non-zero exit code without calling subsequent stages
    - On success log summary line: source file name, total rows, valid rows, rejected rows, rejection rate (rounded to 2 dp); if total rows = 0, rejection rate = 0.00%
    - _Requirements: 5.2, 5.3, 5.4, 5.5, 5.6, 5.7_

  - [ ] 5.3 Implement `main` and wire `if __name__ == "__main__"`
    - Call `parse_args()`, then `run_pipeline(source_file)`, then `sys.exit(exit_code)`
    - _Requirements: 5.1_

- [ ] 6. Checkpoint — verify Gold, Query, and Orchestrator code is complete
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 7. Write unit tests for Quality Checks (`tests/unit/test_quality_checks.py`)
  - [ ] 7.1 Implement hard-failure rule tests using `make_valid_row()`
    - One test per hard-failure rule (14 total): negative `trip_distance`, negative `fare_amount`, negative `total_amount`, `passenger_count <= 0`, `pickup > dropoff`, `PULocationID <= 0`, `DOLocationID <= 0`, and null for each of the 7 nullable-hard-fail columns
    - Each test asserts the mutated row appears in `invalid_df` and not `valid_df`
    - _Requirements: 6.1, 6.2_

  - [ ] 7.2 Write unit tests for row-count invariant and all-valid input
    - Assert `quality_report["valid_rows"] + quality_report["invalid_rows"] == quality_report["total_rows"]`
    - Assert that an all-valid DataFrame produces empty `invalid_df`
    - _Requirements: 6.3, 6.4_

  - [ ] 7.3 Write warning-level check tests
    - One test per warning rule (4 total): null `passenger_count`, zero `trip_distance`, zero duration, duration > 1440 min
    - Assert row stays in `valid_df` and corresponding `quality_report["warnings"]` counter equals 1
    - _Requirements: 6.5_

  - [ ] 7.4 Write `quality_report["hard_failures"]` counter tests
    - Assert counter key equals 1 when exactly one field is mutated
    - _Requirements: 6.6_

- [ ] 8. Write unit tests for Quarantine Writer (`tests/unit/test_quarantine.py`)
  - [ ] 8.1 Implement annotation and column-presence tests
    - Test that a row with negative `fare_amount` gets `validation_errors` containing `"Negative fare amount"`
    - Test that a fully valid row gets `validation_errors = None`
    - Test that output always has `source_file`, `validation_errors`, `rejection_timestamp` columns; `rejection_timestamp` is non-empty ISO-8601 string
    - _Requirements: 7.1, 7.2, 7.3_

  - [ ] 7.2 (see 8.2) Write `write_rejected_file` round-trip test and multi-error test
  - [ ] 8.2 Write `write_rejected_file` round-trip and multi-error tests
    - Test file write round-trip: write then read, assert same row count, expected columns present
    - Test row with two violations gets both error messages joined by `", "`
    - _Requirements: 7.4, 7.6_

  - [ ] 8.3 Write row-count invariant test for `add_validation_errors`
    - **Property 8: Quarantine Row-Count Invariant** — `add_validation_errors()` must return a DataFrame with the same row count as the input
    - **Validates: Requirements 7.5**
    - _Requirements: 7.5_

- [ ] 9. Write unit tests for Ingest function (`tests/unit/test_ingest.py`)
  - [ ] 9.1 Implement mocked S3 idempotency and upload tests using `pytest-mock`
    - Test: `object_exists` returns `True` → `upload_file` not called, `write_ingestion_metadata` called with `status="SUCCESS"`
    - Test: `object_exists` returns `False`, all steps succeed → `upload_file` called once with Bronze bucket, correct object key pattern, source file path
    - _Requirements: 8.1, 8.2_

  - [ ] 9.2 Write validation failure propagation tests
    - Test: `validate_file` raises `FileNotFoundError` → exception propagates, `get_s3_client` not called
    - Test: `validate_schema` raises `ValueError` → exception propagates, `upload_file` not called
    - _Requirements: 8.3, 8.4_

  - [ ] 9.3 Write quarantine-then-Bronze upload order test
    - Test: non-empty `invalid_df` from `run_quality_checks` → `upload_file` first called with `REJECTED_BUCKET`, then with `BRONZE_BUCKET`
    - Mock `write_rejected_file` to avoid disk I/O
    - _Requirements: 8.5_

- [ ] 10. Write unit tests for Silver transformation (`tests/unit/test_silver.py`)
  - [ ] 10.1 Create session-scoped `spark` fixture and implement column-rename tests
    - Fixture: local SparkSession with `master("local[*]")`, no S3 config
    - Test: renamed columns present (`vendor_id`, `ratecode_id`, etc.), originals absent in `silver_df`
    - _Requirements: 9.1, 9.6_

  - [ ] 10.2 Write derived column and rejection logic tests
    - Test: `trip_duration_minutes` computed correctly within tolerance 0.01 min
    - Test: row with `fare_amount < 0` lands in `rejected_df`, not `silver_df`; `rejection_reason` is non-empty
    - Test: all-valid rows → empty `rejected_df`
    - _Requirements: 9.2, 9.3, 9.4_

  - [ ] 10.3 Write Silver row-count invariant property test
    - **Property 9: Silver Transformation Row-Count Invariant** — `silver_df.count() + rejected_df.count() == input.count()`
    - **Validates: Requirements 9.5**
    - _Requirements: 9.5_

- [ ] 11. Write unit tests for Gold aggregations (`tests/unit/test_gold.py`)
  - [ ] 11.1 Create session-scoped `spark` fixture and implement daily revenue tests
    - Fixture: local SparkSession, no S3 config; build small in-memory Silver DataFrame from known values
    - Test: `total_fare_amount` equals exact sum of non-null `fare_amount` values (rounded to 2 dp)
    - _Requirements: 10.1_

  - [ ] 11.2 Implement zone metrics and hourly demand output-shape tests
    - Test: zone metrics row count equals distinct (`pickup_date`, `pickup_location_id`) combinations
    - Test: hourly demand row count equals distinct (`pickup_date`, `pickup_hour`) combinations where `pickup_hour` in [0, 23]
    - Test: rows with `pickup_hour` outside [0, 23] excluded from hourly demand
    - _Requirements: 10.2, 10.3, 10.6_

  - [ ] 11.3 Write daily revenue row-count invariant property test
    - **Property 1: Daily Revenue Row-Count Invariant** — sum of `total_trip_count` across all rows equals input row count
    - **Validates: Requirements 1.6**
    - _Requirements: 1.6, 10.4_

  - [ ] 11.4 Write zone metrics row-count invariant property test
    - **Property 2: Zone Metrics Row-Count Invariant** — sum of `trip_count` across all rows equals input row count
    - **Validates: Requirements 2.4**
    - _Requirements: 2.4, 10.5_

  - [ ] 11.5 Write hourly demand row-count invariant property test
    - **Property 3: Hourly Demand Row-Count Invariant with Filter** — sum of `trip_count` equals count of input rows where `pickup_hour` is in [0, 23] and non-null
    - **Validates: Requirements 3.4, 3.5**
    - _Requirements: 3.4, 3.5_

  - [ ] 11.6 Write Gold aggregation sum consistency property test
    - **Property 10: Gold Aggregation Sum Consistency** — sum of `total_fare_amount` in daily revenue equals sum of `fare_amount` in input (within tolerance 0.01)
    - **Validates: Requirements 10.4**
    - _Requirements: 10.4_

- [ ] 12. Write unit tests for Query Layer (`tests/unit/test_query.py`)
  - [ ] 12.1 Implement `ValueError` on missing env var tests
    - For each of the 3 required env vars, unset it and assert `QueryLayer()` raises `ValueError` containing the variable name
    - **Property 6: Missing Environment Variable Raises ValueError**
    - **Validates: Requirements 4.7**
    - _Requirements: 4.7_

  - [ ] 12.2 Write SQL length validation, schema fidelity, and determinism tests
    - Test: SQL of length 0 or > 10,000 raises `ValueError`
    - Test: `SELECT *` query over a locally written Parquet returns DataFrame with identical column names and dtypes (use DuckDB in-memory + local file, not MinIO)
    - Test: same query twice returns identical row counts and values
    - **Property 4: Query Layer Returns Schema-Faithful DataFrames** — **Validates: Requirements 4.5**
    - **Property 5: Query Layer Determinism** — **Validates: Requirements 4.6**
    - _Requirements: 4.1, 4.5, 4.6_

  - [ ] 12.3 Write parameterized query binding test
    - Test: parameters are bound via `?` placeholder; raw filter string is never interpolated into SQL
    - _Requirements: 4.4_

- [ ] 13. Write unit tests for Orchestrator (`tests/unit/test_pipeline.py`)
  - [ ] 13.1 Implement CLI argument and error-handling tests
    - Test: missing `--source-file` exits non-zero without calling `ingest`
    - Test: path pointing to non-existent file exits non-zero
    - Test: Bronze failure → Silver and Gold not called, non-zero exit
    - Test: Silver failure → Gold not called, non-zero exit
    - _Requirements: 5.1, 5.3, 5.4_

  - [ ] 13.2 Write success-path summary line and rejection rate tests
    - Test: all stages succeed → summary line includes source filename, total/valid/rejected counts, rejection rate rounded to 2 dp
    - **Property 7: Orchestrator Rejection Rate Computation** — test pure helper with `valid + rejected = total`; when `total = 0` rate = 0.00%
    - **Validates: Requirements 5.6**
    - _Requirements: 5.5, 5.6_

- [ ] 14. Checkpoint — ensure all unit tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 15. Write integration smoke test (`tests/integration/test_pipeline_e2e.py`)
  - [ ] 15.1 Implement MinIO availability fixture and full pipeline smoke test
    - `require_minio` autouse fixture: attempt `list_buckets()` within 10 s; skip with message if unreachable
    - Test: run `ingest()` → assert Bronze object key exists in `taxi-bronze` bucket
    - Test: run `transform_to_silver()` + `write_silver()` → assert at least one part-file under `s3a://taxi-silver/taxi/`
    - Test: run Gold job → assert at least one part-file under `s3a://taxi-gold/metrics/daily_revenue/`
    - Test: row-count reconciliation — `bronze_count == silver_count + rejected_count`; fail with counts if not
    - Mark with `@pytest.mark.integration`
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5, 11.6_

- [ ] 16. Write README (`README.md`)
  - [ ] 16.1 Write Prerequisites, Environment Setup, and Running the Pipeline sections
    - Prerequisites: Python ≥ 3.10, Java 8 or 11, Docker, Docker Compose with minimum versions
    - Environment Setup: clone, venv, `pip install -r requirements.txt`, `cp .env.example .env`, create buckets, `docker-compose up -d`
    - Running the Pipeline: exact commands for Bronze ingestion (`python -m src.ingestion.ingest`) and Silver transformation (`python -m src.transformation.silver`)
    - _Requirements: 12.1, 12.2, 12.3_

  - [ ] 16.2 Write Running Tests, Architecture, Bucket Structure, and `.env` sections
    - Running Tests: unit only, integration only, all tests commands
    - Architecture: ASCII/text flow diagram Source → Bronze → Silver → Gold → Query Layer, with rejection path at Bronze and Silver stages
    - Bucket Structure: all 4 buckets, partition keys, example object key patterns
    - Environment Variables: document all 8 required `.env` variables
    - _Requirements: 12.4, 12.5, 12.6, 12.7_

- [ ] 17. Final checkpoint — ensure all tests pass and docs are complete
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for a faster MVP
- Each task references specific requirements for traceability
- The `spark` fixture in Silver and Gold tests must use `master("local[*]")` with no S3 config to avoid network I/O
- Property-based tests (marked with `*`) use `hypothesis` with `min_size=1, max_size=500`; each is tagged with its design Property number
- Integration tests require a live MinIO instance; run with `pytest -m integration`
- Unit tests run with `pytest -m "not integration"`
- `make_valid_row()` from `conftest.py` is the canonical source of valid test rows for all unit tests

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2", "2.1"] },
    { "id": 1, "tasks": ["3.1", "4.1", "5.1"] },
    { "id": 2, "tasks": ["3.2", "3.3", "3.4", "3.5", "4.2", "4.3"] },
    { "id": 3, "tasks": ["3.6", "4.4", "5.2"] },
    { "id": 4, "tasks": ["5.3", "7.1", "8.1", "9.1", "10.1", "11.1", "12.1", "13.1"] },
    { "id": 5, "tasks": ["7.2", "7.3", "7.4", "8.2", "8.3", "9.2", "9.3", "10.2", "10.3", "11.2", "11.3", "11.4", "11.5", "11.6", "12.2", "12.3", "13.2"] },
    { "id": 6, "tasks": ["15.1", "16.1"] },
    { "id": 7, "tasks": ["16.2"] }
  ]
}
```

# Requirements Document

## Introduction

This document covers the completion of a NYC Yellow Taxi data engineering pipeline built on MinIO (S3-compatible local storage) and PySpark. The Bronze ingestion layer and Silver transformation layer are already implemented. The remaining work is to build the Gold aggregation layer, a local query interface over Gold/Silver data, a pipeline orchestrator that drives Bronze → Silver → Gold end-to-end, comprehensive unit and integration test coverage for all existing and new modules, and a complete README. All storage targets are MinIO buckets accessed via the S3A connector.

## Glossary

- **Pipeline**: The end-to-end data processing system: Bronze → Silver → Gold.
- **Bronze_Layer**: Raw, unmodified source Parquet files uploaded to the `taxi-bronze` MinIO bucket, partitioned by `ingestion_date`.
- **Silver_Layer**: Cleaned, renamed, and enriched Parquet data written to the `taxi-silver` MinIO bucket, partitioned by `pickup_date`.
- **Gold_Layer**: Aggregated business-metric Parquet data written to the `taxi-gold` MinIO bucket, partitioned by `pickup_date`.
- **Gold_Job**: The PySpark module (`src/gold/gold.py`) responsible for reading Silver data and writing Gold aggregations.
- **Query_Layer**: The DuckDB-based local query module (`src/query/query.py`) that reads Silver and Gold Parquet files from MinIO via the S3 endpoint and returns results as pandas DataFrames.
- **Orchestrator**: The single entrypoint script (`pipeline.py` at the project root) that executes Bronze ingestion, Silver transformation, and Gold aggregation in sequence.
- **Quality_Checker**: The existing pandas module (`src/validation/quality_checks.py`) that applies hard-failure and warning checks row by row.
- **Quarantine_Writer**: The existing pandas module (`src/validation/quarantine.py`) that annotates rejected rows with error reasons and writes them to a local Parquet file before S3 upload.
- **Ingest_Function**: The `ingest()` function in `src/ingestion/ingest.py` that orchestrates file validation, idempotency check, quality checks, quarantine, S3 upload, and metadata logging.
- **SparkSession**: The PySpark session created by `create_spark_session()` in `src/transformation/silver.py` and reused by the Gold_Job.
- **MinIO**: The local Docker-based S3-compatible object store used as the storage backend for all three layers and the rejected bucket.
- **Rejected_Bucket**: The `taxi-rejected` MinIO bucket that holds quarantined invalid rows from both the Bronze ingestion and Silver transformation stages.
- **Ingestion_Log**: The local Parquet file at `data/metadata/ingestion_log.parquet` that records one row per ingestion event.
- **DuckDB**: An in-process analytical SQL engine used by the Query_Layer to query Parquet files over the S3 endpoint without a separate server.
- **Pickup_Date**: A date value derived from `tpep_pickup_datetime`, used as the partition key in Silver and Gold layers.
- **Rejection_Rate**: The ratio of rejected rows to total input rows, expressed as a percentage.

---

## Requirements

### Requirement 1: Gold Aggregation Layer — Daily Revenue Metrics

**User Story:** As a data analyst, I want daily revenue aggregations written to the Gold layer, so that I can track fare and tip revenue trends without querying raw trip data.

#### Acceptance Criteria

1. WHEN the Gold_Job reads Silver Parquet data for a given `pickup_date` partition, THE Gold_Job SHALL compute the following metrics per `pickup_date`: `total_trip_count` as the count of all rows regardless of null values in metric columns, `total_fare_amount` as the sum of non-null `fare_amount` values, `total_tip_amount` as the sum of non-null `tip_amount` values, `total_amount` as the sum of non-null `total_amount` values, and `avg_fare_amount` as the mean of non-null `fare_amount` values.
2. THE Gold_Job SHALL write the daily revenue aggregation as Parquet to `s3a://taxi-gold/metrics/daily_revenue/`, partitioned by `pickup_date`.
3. WHEN the Gold_Job writes to the `taxi-gold` bucket, THE Gold_Job SHALL use `overwrite` mode so that re-runs on the same partition are idempotent.
4. WHEN the Silver_Layer contains zero rows for a given `pickup_date`, THE Gold_Job SHALL write no output partition for that date rather than writing an empty file.
5. THE Gold_Job SHALL preserve the `pickup_date` column in the output schema alongside all computed metric columns.
6. THE Gold_Job SHALL ensure that the sum of `total_trip_count` values across all `pickup_date` partitions in the Gold output for a single job run equals the total row count of the Silver input read during that same job run.
7. IF the Gold_Job fails to read Silver Parquet data from S3 for a given `pickup_date` partition, THEN THE Gold_Job SHALL abort processing for that partition and emit an error indicating the partition identifier and read failure, without writing any partial output for that partition.

---

### Requirement 2: Gold Aggregation Layer — Zone-Level Trip Metrics

**User Story:** As a data analyst, I want trip counts and average fares broken down by pickup zone, so that I can identify high-demand and high-revenue pickup locations.

#### Acceptance Criteria

1. WHEN the Gold_Job processes Silver data, THE Gold_Job SHALL compute per (`pickup_date`, `pickup_location_id`) group: trip count, average `fare_amount`, average `trip_distance`, and average `trip_duration_minutes`, where `pickup_date` is the calendar date derived from the Silver layer's `pickup_date` field and `trip_duration_minutes` is a numeric column present in the Silver layer.
2. THE Gold_Job SHALL write the zone-level aggregation as Parquet to `s3a://taxi-gold/metrics/zone_metrics/`, partitioned by `pickup_date`.
3. WHEN the Gold_Job writes zone metrics, THE Gold_Job SHALL use dynamic partition overwrite mode, replacing only the `pickup_date` partitions present in the current Silver input while leaving all other existing partitions unchanged.
4. WHEN the Gold_Job processes Silver data, THE Gold_Job SHALL produce zone metrics output in which the sum of `trip_count` values across all (`pickup_date`, `pickup_location_id`) groups equals the total row count of the Silver input DataFrame.
5. IF the Silver input DataFrame contains zero rows, THEN the Gold_Job SHALL write an empty zone metrics output with no rows and no error.

---

### Requirement 3: Gold Aggregation Layer — Hourly Demand Metrics

**User Story:** As an operations analyst, I want trip counts grouped by pickup hour, so that I can understand intraday demand patterns.

#### Acceptance Criteria

1. WHEN the Gold_Job processes Silver data from `s3a://taxi-silver/` and the Silver input contains at least one row, THE Gold_Job SHALL compute per (`pickup_date`, `pickup_hour`) group: `trip_count` as the count of rows in that group, and `avg_fare_amount` as the mean of non-null `fare_amount` values in that group.
2. THE Gold_Job SHALL write the hourly demand aggregation as Parquet to `s3a://taxi-gold/metrics/hourly_demand/`, partitioned by `pickup_date`, using dynamic partition overwrite mode so that only the `pickup_date` partitions present in the current Silver input are replaced.
3. WHEN the Gold_Job writes hourly demand metrics, THE Gold_Job SHALL use `overwrite` mode.
4. IF the Silver input contains rows where `pickup_hour` is outside the integer range [0, 23], THEN THE Gold_Job SHALL silently exclude those rows from the hourly demand output and not raise an error; the excluded rows SHALL NOT be counted in any `trip_count` output.
5. WHEN the Gold_Job produces hourly demand output, THE Gold_Job SHALL ensure that the sum of `trip_count` values across all (`pickup_date`, `pickup_hour`) groups equals the count of Silver input rows where `pickup_hour` is in [0, 23] and `pickup_hour` is non-null.

---

### Requirement 4: Query Layer — DuckDB-Based Local Analytics

**User Story:** As a data engineer, I want to run SQL queries against Silver and Gold Parquet data on MinIO without deploying AWS Athena, so that I can validate pipeline outputs and build exploratory analyses locally.

#### Acceptance Criteria

1. THE Query_Layer SHALL accept a SQL string of 1 to 10,000 characters and a target dataset identifier string matching the pattern `"<layer>"` or `"<layer>/<sub-path>"` (e.g., `"silver"`, `"gold/daily_revenue"`), and return the result as a pandas DataFrame.
2. WHEN the Query_Layer initializes a connection to MinIO, THE Query_Layer SHALL read the S3 endpoint URL, access key, and secret key exclusively from environment variables via the existing `config.py` module, without falling back to default or hardcoded values.
3. IF the SQL query references a path that does not exist in MinIO, THEN THE Query_Layer SHALL raise a `ValueError` containing the unresolved path string, and no partial result SHALL be returned.
4. IF one or more filter values are supplied by the caller as query parameters, THEN THE Query_Layer SHALL bind those values through a parameterized query interface, and the raw filter strings SHALL NOT be interpolated directly into the SQL string.
5. WHEN a SQL query executes successfully against MinIO, THE Query_Layer SHALL return a pandas DataFrame whose column names and column dtypes match exactly those of the queried Parquet file's schema, with no columns added, removed, or recast.
6. WHEN the same valid SQL string and dataset identifier are executed twice in sequence against an unchanged MinIO path, THE Query_Layer SHALL return DataFrames with identical row counts and identical column values in every row.
7. IF any required environment variable (`S3 endpoint URL`, `access key`, or `secret key`) is absent or empty at initialization time, THEN THE Query_Layer SHALL raise a `ValueError` indicating which variable is missing, and no connection attempt SHALL be made.

---

### Requirement 5: Pipeline Orchestration

**User Story:** As a data engineer, I want a single entrypoint that runs Bronze ingestion, Silver transformation, and Gold aggregation in sequence, so that I can trigger a full pipeline run with one command.

#### Acceptance Criteria

1. THE Orchestrator SHALL accept a source file path as a required command-line argument (`--source-file`) where the value is a non-empty string representing a path to an existing readable file; IF the argument is absent or the path does not resolve to an existing readable file, THEN THE Orchestrator SHALL emit an error message indicating the invalid or missing argument and exit with a non-zero exit code without executing any pipeline step.
2. WHEN invoked with a valid `--source-file` argument, THE Orchestrator SHALL execute the pipeline in the following fixed order: (1) Bronze ingestion via `ingest()`, (2) Silver transformation via `transform_to_silver()` followed by `write_silver()`, (3) Gold aggregation via the Gold Job; no step SHALL begin before the preceding step has completed successfully.
3. IF the Bronze ingestion step raises an exception, THEN THE Orchestrator SHALL log an error message indicating that Bronze ingestion failed and the reason, skip the Silver and Gold steps, and exit with a non-zero exit code.
4. IF the Silver transformation step raises an exception, THEN THE Orchestrator SHALL log an error message indicating that Silver transformation failed and the reason, skip the Gold step, and exit with a non-zero exit code.
5. IF the Gold aggregation step raises an exception, THEN THE Orchestrator SHALL log an error message indicating that Gold aggregation failed and the reason, and exit with a non-zero exit code.
6. WHEN all three steps complete without exception, THE Orchestrator SHALL log a single summary line containing: the source file name (base name only, without directory path), total rows processed as a non-negative integer, valid rows as a non-negative integer, rejected rows as a non-negative integer, and Rejection Rate expressed as a percentage rounded to two decimal places, where Rejection Rate = (rejected rows / total rows processed) × 100; IF total rows processed is zero, THEN Rejection Rate SHALL be reported as 0.00%.
7. THE Orchestrator SHALL reuse the same SparkSession instance across the Silver and Gold steps without creating a second SparkSession; the single SparkSession SHALL be created before the Silver step begins and remain active until the Gold step completes or an exception causes an early exit.

---

### Requirement 6: Unit Tests — Quality Checks

**User Story:** As a developer, I want unit tests for `quality_checks.py`, so that I can verify every hard-failure rule and warning is applied correctly without running the full pipeline.

#### Acceptance Criteria

1. THE Test_Suite SHALL contain one test function per hard-failure rule defined in `run_quality_checks()`, each building a single-row DataFrame using `make_valid_row()` with exactly one field mutated to trigger that rule: negative `trip_distance`, negative `fare_amount`, negative `total_amount`, `passenger_count <= 0`, `tpep_pickup_datetime > tpep_dropoff_datetime`, `PULocationID <= 0`, `DOLocationID <= 0`, and `None` for each of the seven nullable-hard-fail columns (`VendorID`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `PULocationID`, `DOLocationID`, `fare_amount`, `total_amount`).
2. WHEN a row violates exactly one hard-failure rule, THE Test_Suite SHALL assert that the row appears in `invalid_df` and not in `valid_df`.
3. WHEN all rows in the input DataFrame are valid, THE Test_Suite SHALL assert that `invalid_df` is empty and `valid_df` has the same row count as the input.
4. THE Test_Suite SHALL assert that `quality_report["valid_rows"] + quality_report["invalid_rows"] == quality_report["total_rows"]` for all inputs (row-count invariant).
5. THE Test_Suite SHALL contain one test function per warning-level check (`null passenger_count`, `zero trip_distance`, `zero duration`, `duration > 1440 minutes`), each building a single-row DataFrame using `make_valid_row()` with exactly one field mutated to trigger that warning, and asserting that the row appears in `valid_df` and the corresponding counter in `quality_report["warnings"]` equals 1.
6. IF a single-row DataFrame is built from `make_valid_row()` with exactly one field mutated to an invalid value, THEN THE Test_Suite SHALL assert that `invalid_df` row count equals 1 and the corresponding key in `quality_report["hard_failures"]` equals 1, using the key names defined in `run_quality_checks()`: `negative_trip_distance`, `negative_fare`, `negative_total`, `invalid_passenger_count`, `pickup_after_dropoff`, `invalid_PULocationID`, `invalid_DOLocationID`, and the seven `null_*` keys.

---

### Requirement 7: Unit Tests — Quarantine Writer

**User Story:** As a developer, I want unit tests for `quarantine.py`, so that I can confirm rejected rows are annotated with the correct error reasons and written to disk reliably.

#### Acceptance Criteria

1. WHEN `add_validation_errors()` is called with a `source_file` string and a DataFrame containing a row that has a negative `fare_amount`, THE Test_Suite SHALL assert that the `validation_errors` column for that row contains the substring `"Negative fare amount"`.
2. WHEN `add_validation_errors()` is called with a `source_file` string and a DataFrame containing a fully valid row (as defined by `make_valid_row()` with no overrides), THE Test_Suite SHALL assert that the `validation_errors` column for that row is `None`.
3. THE Test_Suite SHALL assert that `add_validation_errors()` adds the `source_file`, `validation_errors`, and `rejection_timestamp` columns to the output DataFrame, and that `rejection_timestamp` is a non-empty ISO-8601 UTC timestamp string.
4. WHEN `write_rejected_file()` is called with a valid DataFrame and a path under `tmp_path`, THE Test_Suite SHALL assert that the output file exists, can be read back as a Parquet file with the same row count as the input, and that the read-back DataFrame contains the `validation_errors`, `source_file`, and `rejection_timestamp` columns.
5. FOR ALL DataFrames with at least one invalid row, calling `add_validation_errors()` SHALL return a DataFrame with the same number of rows as the input (row-count invariant: no rows are dropped or added).
6. WHEN `add_validation_errors()` is called with a row that violates more than one hard-failure rule (e.g., negative `fare_amount` AND negative `total_amount`), THE Test_Suite SHALL assert that the `validation_errors` column for that row contains both error messages joined by `", "`.

---

### Requirement 8: Unit Tests — Ingest Function (Mocked S3)

**User Story:** As a developer, I want unit tests for `ingest()` with mocked S3 calls, so that I can verify the ingestion pipeline logic without requiring a running MinIO instance.

#### Acceptance Criteria

1. WHEN `get_s3_client()` is mocked and `object_exists()` is mocked to return `True`, THE Test_Suite SHALL assert that `upload_file()` is not called and `write_ingestion_metadata()` is called with `status="SUCCESS"` and `target_bucket` equal to the Bronze bucket name.
2. WHEN `get_s3_client()` is mocked, `object_exists()` returns `False`, and all steps succeed, THE Test_Suite SHALL assert that `upload_file()` is called exactly once with the Bronze bucket name, the object key matching the pattern `taxi/ingestion_date=<YYYY-MM-DD>/<filename>`, and the source file path.
3. IF `validate_file()` raises `FileNotFoundError`, THEN THE Test_Suite SHALL assert that the exception propagates from `ingest()` and `get_s3_client()` is not called.
4. IF `validate_schema()` raises `ValueError`, THEN THE Test_Suite SHALL assert that the exception propagates from `ingest()` and `upload_file()` is not called.
5. WHEN `get_s3_client()` is mocked, `object_exists()` returns `False`, and `run_quality_checks()` returns a non-empty `invalid_df`, THE Test_Suite SHALL assert that `upload_file()` is called first with the Rejected_Bucket and the quarantine key before being called with the Bronze bucket, and that `write_rejected_file()` is mocked to avoid writing to disk.
6. THE Test_Suite SHALL mock `get_s3_client()`, `object_exists()`, and `upload_file()` using `pytest-mock` for all test cases; no real MinIO connection SHALL be required.

---

### Requirement 9: Unit Tests — Silver Transformation (Mocked Spark)

**User Story:** As a developer, I want unit tests for `transform_to_silver()`, so that I can verify column renaming, derived column computation, and rejection logic on small in-memory DataFrames.

#### Acceptance Criteria

1. WHEN `transform_to_silver()` is called with a Spark DataFrame containing the columns `VendorID`, `RatecodeID`, `PULocationID`, `DOLocationID`, `Airport_fee`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `trip_distance`, `fare_amount`, `total_amount`, `passenger_count`, and at least one valid row, THE Test_Suite SHALL assert that `silver_df` contains the columns `vendor_id`, `ratecode_id`, `pickup_location_id`, `dropoff_location_id`, and `airport_fee`, and that none of `VendorID`, `RatecodeID`, `PULocationID`, `DOLocationID`, `Airport_fee` appear in `silver_df`.
2. WHEN `transform_to_silver()` is called with a valid row where `tpep_pickup_datetime` and `tpep_dropoff_datetime` are set to known timestamp values, THE Test_Suite SHALL assert that `trip_duration_minutes` equals `(unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 60` within an absolute tolerance of 0.01 minutes.
3. WHEN a row has `fare_amount` less than 0, THE Test_Suite SHALL assert that the row appears in `rejected_df` and not in `silver_df`, and that the `rejection_reason` column value for that row is a non-empty string of length ≥ 1.
4. WHEN all rows in the input DataFrame satisfy the valid-row definition from `make_valid_row()` in `conftest.py` with no overrides, THE Test_Suite SHALL assert that `rejected_df` contains 0 rows and `silver_df` row count equals the input DataFrame row count.
5. WHEN `transform_to_silver()` is called with any input DataFrame containing 1 or more rows, THE Test_Suite SHALL assert that `silver_df.count() + rejected_df.count()` equals the input DataFrame row count.
6. WHEN the Test_Suite initializes the SparkSession for `transform_to_silver()` tests, THE Test_Suite SHALL create a local SparkSession without configuring any S3 or MinIO endpoint, such that all tests complete without network I/O to an object store.

---

### Requirement 10: Unit Tests — Gold Aggregations

**User Story:** As a developer, I want unit tests for the Gold_Job aggregation logic, so that I can verify metric computations on small in-memory DataFrames without writing to MinIO.

#### Acceptance Criteria

1. WHEN the daily revenue aggregation is computed on a DataFrame with known values, THE Test_Suite SHALL assert that `total_fare_amount` equals the exact sum of all non-null `fare_amount` values in the input, rounded to 2 decimal places.
2. WHEN the zone metrics aggregation is computed, THE Test_Suite SHALL assert that the number of output rows equals the number of distinct (`pickup_date`, `pickup_location_id`) combinations in the input.
3. WHEN the hourly demand aggregation is computed, THE Test_Suite SHALL assert that the number of output rows equals the number of distinct (`pickup_date`, `pickup_hour`) combinations in the input where `pickup_hour` is in [0, 23].
4. FOR ALL input DataFrames where every row has a non-null `fare_amount` and a non-null `pickup_date`, the sum of `total_trip_count` across all rows of the daily revenue output SHALL equal the input row count (row-count invariant).
5. FOR ALL input DataFrames where every row has a non-null `pickup_location_id` and a non-null `pickup_date`, the sum of `trip_count` across all rows of the zone metrics output SHALL equal the input row count (row-count invariant).
6. IF the input DataFrame contains rows where `pickup_hour` is outside [0, 23], THEN THE Gold_Job SHALL exclude those rows from the hourly demand output so that only rows with `pickup_hour` in [0, 23] appear in the output.

---

### Requirement 11: Integration Test — End-to-End Pipeline Smoke Test

**User Story:** As a developer, I want an integration test that runs the full Bronze → Silver → Gold pipeline against a real MinIO instance, so that I can confirm all three layers write correctly and the row counts reconcile.

#### Acceptance Criteria

1. WHEN the integration test runs with a valid source Parquet file and a live MinIO instance reachable on the configured endpoint within 10 seconds, THE Test_Suite SHALL assert that exactly one object whose key matches the expected Bronze path exists in the `taxi-bronze` bucket after `ingest()` completes.
2. IF the MinIO instance is unreachable or returns a connection error at test startup, THEN THE Test_Suite SHALL skip the test with a message indicating that the MinIO instance is unavailable and mark the result as skipped rather than failed.
3. WHEN the integration test runs Silver transformation, THE Test_Suite SHALL assert that at least one Parquet part-file exists under `s3a://taxi-silver/taxi/` after `write_silver()` completes within 60 seconds.
4. WHEN the integration test runs Gold aggregation, THE Test_Suite SHALL assert that at least one Parquet part-file exists under `s3a://taxi-gold/metrics/daily_revenue/` after the Gold_Job completes within 60 seconds.
5. WHEN the row-count reconciliation check runs, THE Test_Suite SHALL assert that the Bronze row count equals the Silver row count plus the Rejected row count, where each count is read from the outputs written during the same test run, and SHALL fail with a message indicating the Bronze, Silver, and Rejected counts if the assertion does not hold.
6. THE Test_Suite SHALL be marked with a `pytest` marker (`@pytest.mark.integration`) so it can be excluded from unit test runs via `-m "not integration"`.

---

### Requirement 12: README Completion

**User Story:** As a new contributor, I want a complete README with setup instructions and architecture documentation, so that I can run the pipeline locally within 15 minutes of cloning the repository.

#### Acceptance Criteria

1. THE README SHALL contain a Prerequisites section listing: Python ≥ 3.10, Java 8 or 11 (required for PySpark 3.5.1), Docker, and Docker Compose, with minimum version numbers explicitly stated.
2. THE README SHALL contain an Environment Setup section with the exact commands to: clone the repository, create and activate a virtual environment, install dependencies from `requirements.txt`, copy `.env.example` to `.env` (where `.env.example` is a tracked file in the repository root), create the required MinIO buckets (`taxi-bronze`, `taxi-silver`, `taxi-gold`, `taxi-rejected`), and start MinIO with `docker-compose up -d`.
3. THE README SHALL contain a Running the Pipeline section with the exact command to invoke Bronze ingestion via `python -m src.ingestion.ingest` and Silver transformation via `python -m src.transformation.silver` for a given source file.
4. THE README SHALL contain a Running Tests section with commands to run unit tests only (`pytest -m "not integration"`), integration tests only (`pytest -m integration`), and all tests (`pytest`).
5. THE README SHALL contain an Architecture section with a text diagram showing the flow: Source File → Bronze (MinIO) → Silver (MinIO) → Gold (MinIO) → Query Layer, with the rejection path (invalid rows → Rejected_Bucket) shown at both the Bronze and Silver stages.
6. THE README SHALL contain a Bucket Structure section listing all four MinIO buckets (`taxi-bronze`, `taxi-silver`, `taxi-gold`, `taxi-rejected`), their partition keys (`ingestion_date` for Bronze, `pickup_date` for Silver and Gold, `pickup_date` under `rejection_stage=silver` prefix for Silver-stage rejections), and example object key patterns.
7. THE README SHALL document all required `.env` variables: `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `BRONZE_BUCKET`, `SILVER_BUCKET`, `GOLD_BUCKET`, `REJECTED_BUCKET`, and `METADATA_PATH`.
8. THE repository root SHALL contain a `.env.example` file listing all required environment variable keys with placeholder values (not real credentials) so that the `cp .env.example .env` step in criterion 2 is executable immediately after cloning.

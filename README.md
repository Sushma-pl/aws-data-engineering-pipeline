# AWS Data Engineering Pipeline

A PySpark + MinIO-based data pipeline for NYC Yellow Taxi trip data that ingests raw parquet files into Bronze, validates and quarantines bad rows, transforms data into Silver, and aggregates metrics into Gold.

## Status

This project is partially implemented and is in active completion. The core pipeline layers are present:

- Bronze ingestion
- Silver transformation
- Gold aggregation
- Query layer for DuckDB analytics
- CLI orchestrator

The repository still contains some incomplete or unfinalized work compared to the original completion plan, so it should be treated as a working project under active refinement rather than a fully finished production-ready deployment.

## Architecture

```text
Source Parquet
    │
    ▼
File validation
    │
    ├── invalid -> reject/quarantine
    │
    ▼
Bronze (MinIO / S3)
    │
    ▼
Silver transformation
    │
    ├── rejected rows -> quarantine
    │
    ▼
Gold aggregation
    │
    ▼
Query layer (DuckDB)
```

## Repository layout

```text
AWS_data_engineering_pipeline/
├── pipeline.py
├── README.md
├── requirements.txt
├── pytest.ini
├── .env.example
├── docker-compose.yml
├── data/
│   ├── metadata/
│   ├── rejected/
│   └── source/
├── src/
│   ├── ingestion/
│   ├── transformation/
│   ├── validation/
│   ├── gold/
│   └── query/
├── tests/
│   ├── unit/
│   └── integration/
├── Rough/
└── .kiro/
```

## Prerequisites

- Python 3.10+
- Java 8 or 11 (required by PySpark)
- Docker + Docker Compose
- MinIO access via local Docker services
- AWS-compatible S3 semantics for local MinIO testing

## Environment configuration

1. Copy the template environment file:

```bash
cp .env.example .env
```

2. Update the values in `.env` for your local environment:

```env
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
BRONZE_BUCKET=taxi-bronze
SILVER_BUCKET=taxi-silver
GOLD_BUCKET=taxi-gold
REJECTED_BUCKET=taxi-rejected
METADATA_PATH=data/metadata/ingestion_log.parquet
```

3. Start local services if required:

```bash
docker-compose up -d
```

## Running the pipeline

### 1. Bronze ingestion

```bash
python pipeline.py --source-file data/source/<your_file>.parquet
```

### 2. Silver transformation

```bash
python -m src.transformation.silver
```

### 3. Gold aggregation

```bash
python -m src.gold.gold
```

## Data quality rules

The validator applies hard-fail checks and warning checks, including:

### Hard failure rules

- negative trip distance
- negative fare amount
- negative total amount
- passenger count <= 0
- pickup after dropoff
- invalid pickup or dropoff location IDs
- null required values such as vendor ID and timestamps

### Warning rules

- null passenger_count
- trip_distance = 0
- trip duration = 0
- trip duration > 24 hours

## MinIO buckets

The project expects these buckets:

- `taxi-bronze` — raw uploaded files
- `taxi-silver` — cleaned and validated transformed data
- `taxi-gold` — aggregated analytical outputs
- `taxi-rejected` — invalid records quarantined during validation

## Testing

Run the test suite from the project root:

```bash
pytest -q
```

For unit-only runs:

```bash
pytest tests -m "not integration" -q
```

For integration-only runs:

```bash
pytest tests -m integration -q
```

## Notes

- The project is designed to work with a local MinIO-compatible S3 endpoint.
- The pipeline is structured for Bronze → Silver → Gold processing.
- Some project requirements listed in the internal spec are still incomplete, so completion should be considered ongoing rather than final.

## Contributing

When making updates, keep the README aligned with the actual code and environment requirements. The README should always describe the current repository state, not an aspirational one.

# NYC Yellow Taxi Data Engineering Pipeline

A PySpark + MinIO-based data pipeline for NYC Yellow Taxi trip data. Raw Parquet files are ingested into a Bronze layer, validated and cleaned into a Silver layer, aggregated into a Gold layer, and made queryable via a DuckDB-powered query interface.

---

## Prerequisites

| Tool | Minimum Version | Notes |
|------|----------------|-------|
| Python | 3.10 | Tested with 3.10+ |
| Java | 8 or 11 | Required for PySpark 3.5.1 |
| Docker | 20.10+ | Runs the local MinIO instance |
| Docker Compose | 2.0+ | Manages the MinIO container |

> Java 8 or 11 must be on your `PATH` before starting PySpark. Java 17+ is not supported by PySpark 3.5.x without additional flags.

---

## Environment Setup

### 1. Clone the repository

```bash
git clone <repository-url>
cd AWS_data_engineering_pipeline
```

### 2. Create and activate a virtual environment

```bash
python -m venv .venv

# Linux / macOS
source .venv/bin/activate

# Windows
.venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

```bash
cp .env.example .env
```

Open `.env` and verify the values match your local setup. The defaults work with the provided `docker-compose.yml`:

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

### 5. Start MinIO

```bash
docker-compose up -d
```

MinIO will be accessible at `http://localhost:9000` (API) and `http://localhost:9001` (console).

### 6. Create the required MinIO buckets

Open the MinIO console at `http://localhost:9001` and create the four buckets listed below, **or** run the following commands with the MinIO client (`mc`) after pointing it at your instance:

```bash
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/taxi-bronze
mc mb local/taxi-silver
mc mb local/taxi-gold
mc mb local/taxi-rejected
```

---

## Running the Pipeline

### Option A — Full pipeline via the orchestrator (recommended)

```bash
python pipeline.py --source-file data/source/yellow_tripdata_2026-05.parquet
```

This runs all three stages in sequence: Bronze ingestion → Silver transformation → Gold aggregation.

On success the orchestrator prints a summary line:

```
Pipeline complete | file=yellow_tripdata_2026-05.parquet | total_rows=123456 | valid_rows=120000 | rejected_rows=3456 | rejection_rate=2.80%
```

### Option B — Run each stage independently

**Bronze ingestion only:**

```bash
python -m src.ingestion.ingest
```

**Silver transformation only:**

```bash
python -m src.transformation.silver
```

**Gold aggregation only:**

```bash
python -m src.gold.gold
```

---

## Running Tests

### Unit tests only (no MinIO required)

```bash
pytest -m "not integration"
```

### Integration tests only (requires live MinIO)

```bash
pytest -m integration
```

### All tests

```bash
pytest
```

> Integration tests will be **automatically skipped** (not failed) if MinIO is unreachable at test startup.

---

## Architecture

```
Source File
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│  Bronze Layer  (MinIO: taxi-bronze)                     │
│  • File validation                                      │
│  • Idempotency check (skip if already uploaded)         │
│  • Schema validation                                    │
│  • Quality checks                                       │
│    ├── Invalid rows ──► taxi-rejected (Bronze stage)    │
│  • Upload raw Parquet partitioned by ingestion_date     │
└─────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│  Silver Layer  (MinIO: taxi-silver)                     │
│  • Column rename & standardisation (PySpark)            │
│  • Derived columns: trip_duration_minutes,              │
│    pickup_date, pickup_hour                             │
│  • Rejection filter                                     │
│    ├── Invalid rows ──► taxi-rejected (Silver stage)    │
│  • Write Parquet partitioned by pickup_date             │
└─────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│  Gold Layer  (MinIO: taxi-gold)                         │
│  • Daily revenue metrics (per pickup_date)              │
│  • Zone-level trip metrics (per pickup_date + zone)     │
│  • Hourly demand metrics (per pickup_date + hour)       │
│  • Write Parquet partitioned by pickup_date             │
└─────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│  Query Layer  (DuckDB — in-process, no server)          │
│  • SQL queries over Silver and Gold Parquet via httpfs  │
│  • Returns results as pandas DataFrames                 │
└─────────────────────────────────────────────────────────┘
```

---

## Bucket Structure

| Bucket | Description | Partition Key | Example Object Key |
|--------|-------------|---------------|--------------------|
| `taxi-bronze` | Raw uploaded Parquet files (unchanged) | `ingestion_date` | `taxi/ingestion_date=2026-05-01/yellow_tripdata_2026-05.parquet` |
| `taxi-silver` | Cleaned, renamed, enriched Parquet files | `pickup_date` | `taxi/pickup_date=2026-05-01/part-00000-abc.parquet` |
| `taxi-gold` | Aggregated metric Parquet files | `pickup_date` | `metrics/daily_revenue/pickup_date=2026-05-01/part-00000-def.parquet` |
| `taxi-rejected` | Invalid rows quarantined during Bronze or Silver stage | `pickup_date` | `taxi/ingestion_date=2026-05-01/yellow_tripdata_2026-05_invalid.parquet` (Bronze) · `taxi/rejection_stage=silver/pickup_date=2026-05-01/part-00000-ghi.parquet` (Silver) |

---

## Environment Variables

All variables are required. Copy `.env.example` to `.env` and fill in the values.

| Variable | Description | Example |
|----------|-------------|---------|
| `MINIO_ENDPOINT` | Full URL of the MinIO S3-compatible API endpoint | `http://localhost:9000` |
| `MINIO_ACCESS_KEY` | MinIO access key (username) | `minioadmin` |
| `MINIO_SECRET_KEY` | MinIO secret key (password) | `minioadmin` |
| `BRONZE_BUCKET` | Name of the Bronze MinIO bucket | `taxi-bronze` |
| `SILVER_BUCKET` | Name of the Silver MinIO bucket | `taxi-silver` |
| `GOLD_BUCKET` | Name of the Gold MinIO bucket | `taxi-gold` |
| `REJECTED_BUCKET` | Name of the Rejected MinIO bucket | `taxi-rejected` |
| `METADATA_PATH` | Local path to the ingestion metadata log Parquet file | `data/metadata/ingestion_log.parquet` |

---

## Data Quality Rules

### Hard failures (row is rejected)

- Negative `trip_distance`, `fare_amount`, or `total_amount`
- `passenger_count` ≤ 0
- `tpep_pickup_datetime` > `tpep_dropoff_datetime`
- `PULocationID` or `DOLocationID` ≤ 0
- Null values in any of: `VendorID`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `PULocationID`, `DOLocationID`, `fare_amount`, `total_amount`

### Warnings (row is kept, counter incremented)

- Null `passenger_count`
- `trip_distance` = 0
- Trip duration = 0 minutes
- Trip duration > 1 440 minutes (24 hours)

---

## Repository Layout

```
AWS_data_engineering_pipeline/
├── pipeline.py                    # CLI orchestrator (Bronze → Silver → Gold)
├── README.md
├── requirements.txt
├── pytest.ini                     # pytest markers (integration)
├── .env.example                   # environment variable template
├── docker-compose.yml             # MinIO service definition
├── data/
│   ├── metadata/                  # ingestion_log.parquet
│   ├── rejected/                  # locally written quarantine files
│   └── source/                    # source Parquet files
├── src/
│   ├── ingestion/
│   │   ├── config.py              # environment variable loader
│   │   ├── ingest.py              # Bronze ingestion orchestrator
│   │   ├── metadata.py            # ingestion log writer
│   │   └── s3_client.py           # boto3 S3 helpers
│   ├── transformation/
│   │   └── silver.py              # PySpark Silver transformation
│   ├── validation/
│   │   ├── quality_checks.py      # hard-failure and warning rules
│   │   ├── quarantine.py          # rejected-row annotator and writer
│   │   └── validator.py           # file and schema validators
│   ├── gold/
│   │   └── gold.py                # PySpark Gold aggregation job
│   └── query/
│       └── query.py               # DuckDB query layer
└── tests/
    ├── conftest.py                # shared fixtures and make_valid_row()
    ├── unit/
    │   ├── test_quality_checks.py
    │   ├── test_quarantine.py
    │   ├── test_ingest.py
    │   ├── test_silver.py
    │   ├── test_gold.py
    │   ├── test_query.py
    │   └── test_pipeline.py
    └── integration/
        └── test_pipeline_e2e.py   # end-to-end smoke test (requires MinIO)
```

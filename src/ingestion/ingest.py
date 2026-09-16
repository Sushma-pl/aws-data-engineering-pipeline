from datetime import datetime, timezone
from pathlib import Path
import logging

from src.validation.validator import validate_file, validate_schema
from .s3_client import get_s3_client, object_exists, upload_file
from src.validation.quality_checks import run_quality_checks
from src.validation.quarantine import add_validation_errors, write_rejected_file
from .config import BRONZE_BUCKET, REJECTED_BUCKET
from .metadata import write_ingestion_metadata   # fixed: was write_ingestion_matadata

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def ingest(file_path: str) -> None:
    """
    Ingest a source Parquet file into the Bronze S3/MinIO bucket.

    Pipeline:
        1. File-level validation (exists, .parquet, non-empty)
        2. Idempotency check: skip upload if already in Bronze
        3. Schema validation (required columns present)
        4. Data quality checks (hard failures separated from valid rows)
        5. Quarantine invalid rows -> taxi-rejected bucket
        6. Upload raw source file -> taxi-bronze bucket
        7. Write ingestion metadata to ingestion_log.parquet

    Args:
        file_path: Local path to the source Parquet file.
    """
    file = Path(file_path)

    # ── 1. File validation ────────────────────────────────────────────────────
    try:
        validate_file(file_path)
        logger.info("File validation passed | file=%s", file.name)
    except Exception as e:
        logger.exception("File validation failed | file=%s | error=%s", file.name, e)
        raise

    file_name = file.name
    file_size = file.stat().st_size
    ingestion_timestamp = datetime.now(timezone.utc).isoformat()
    ingestion_date = ingestion_timestamp[:10]
    object_key = f"taxi/ingestion_date={ingestion_date}/{file_name}"

    logger.info("Starting ingestion | file=%s | size=%d bytes", file_name, file_size)

    # ── 2. Idempotency check ──────────────────────────────────────────────────
    s3_client = get_s3_client()

    if object_exists(s3_client, BRONZE_BUCKET, object_key):
        logger.info(
            "File already exists. Skipping upload | s3://%s/%s",
            BRONZE_BUCKET, object_key,
        )
        write_ingestion_metadata(
            file_path=file_path,
            ingestion_timestamp=ingestion_timestamp,
            file_size=file_size,
            target_bucket=BRONZE_BUCKET,
            target_key=object_key,
            status="SUCCESS",
        )
        return

    # ── 3. Schema validation ──────────────────────────────────────────────────
    try:
        df = validate_schema(file_path)
        logger.info("Schema validation passed | file=%s", file_name)
    except Exception as e:
        logger.exception("Schema validation failed | file=%s | error=%s", file_name, e)
        raise

    # ── 4. Quality checks ─────────────────────────────────────────────────────
    try:
        valid_df, invalid_df, quality_report = run_quality_checks(df)
        logger.info(
            "Quality checks done | file=%s | total=%d | valid=%d | invalid=%d",
            file_name,
            quality_report["total_rows"],
            quality_report["valid_rows"],
            quality_report["invalid_rows"],
        )
    except Exception as e:
        logger.exception("Quality checks failed | file=%s | error=%s", file_name, e)
        raise

    # ── 5. Quarantine invalid rows ────────────────────────────────────────────
    if not invalid_df.empty:
        invalid_df = add_validation_errors(invalid_df, source_file=file_name)

        logger.warning(
            "Invalid records found | file=%s | count=%d",
            file_name, len(invalid_df),
        )

        rejected_file = Path(
            f"data/rejected/{file_name.replace('.parquet', '_invalid.parquet')}"
        )
        write_rejected_file(invalid_df, str(rejected_file))

        rejected_key = f"taxi/ingestion_date={ingestion_date}/{rejected_file.name}"
        upload_file(s3_client, REJECTED_BUCKET, rejected_key, str(rejected_file))

        logger.info(
            "Rejected records uploaded | s3://%s/%s | count=%d",
            REJECTED_BUCKET, rejected_key, len(invalid_df),
        )

    # ── 6. Upload raw Bronze file ─────────────────────────────────────────────
    try:
        upload_file(s3_client, BRONZE_BUCKET, object_key, file_path)
        logger.info(
            "Bronze upload successful | s3://%s/%s",
            BRONZE_BUCKET, object_key,
        )
    except Exception as e:
        logger.exception("Bronze upload failed | file=%s | error=%s", file_name, e)
        raise

    # ── 7. Write ingestion metadata ───────────────────────────────────────────
    write_ingestion_metadata(
        file_path=file_path,
        ingestion_timestamp=ingestion_timestamp,
        file_size=file_size,
        target_bucket=BRONZE_BUCKET,
        target_key=object_key,
        status="SUCCESS",
    )


if __name__ == "__main__":
    ingest("data/source/yellow_tripdata_2026-05.parquet")

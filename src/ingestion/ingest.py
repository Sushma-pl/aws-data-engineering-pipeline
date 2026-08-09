from datetime import datetime, timezone
from pathlib import Path
import logging

from src.validation.validator import (
    validate_file, 
    validate_schema
)


from .s3_client import (
    get_s3_client,
    object_exists,
    upload_file,
)

from src.validation.quality_checks import run_quality_checks
from src.validation.quarantine import add_validation_errors, write_rejected_file


from .config import (
    BRONZE_BUCKET,
    REJECTED_BUCKET
)

from .metadata import write_ingestion_matadata

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger= logging.getLogger(__name__)


def ingest(file_path:str):
    """
    Ingest a file to the S3 bucket.
    
    Args:
        file_path (str): The path to the file to be ingested.
    """
    file = Path(file_path)

    # 1. validate source file
    try:
        validate_file(file_path)

        logger.info(
            "File-level validation successful | file='%s'",
            file.name,
        )

    except Exception as e:
        logger.exception(
            "File-level validation failed for file | file='%s' | error='%s'",
            file.name,
            str(e)
        )
        raise

    file_name = file.name

    # 2. Get metadata
    file_size= file.stat().st_size
    ingestion_timestamp = datetime.now(timezone.utc).isoformat()

    ingestion_date = ingestion_timestamp[:10]  # Extract date in YYYY-MM-DD format

    # 3. create target key for s3
    object_key = f"taxi/ingestion_date={ingestion_date}/{file_name}"

    logger.info(
            "Starting ingestion | file='%s' | size: %d bytes ",
            file_name,
            file_size
    )

    # 4 Create S3 client
    s3_client = get_s3_client()

    # 5. Check idempotency: check if the file already exists in the S3 bucket

    if object_exists(
        s3_client, 
        BRONZE_BUCKET, 
        object_key
    ):
        logger.info(
            "File already exists. Skipping upload | s3://%s/%s.",
            BRONZE_BUCKET,
            object_key
        )

        # check metadata: this funstion will check record exist or not
        write_ingestion_matadata(
            file_path=file_path,
            ingestion_timestamp=ingestion_timestamp,
            file_size=file_size,
            target_bucket=BRONZE_BUCKET,
            target_key=object_key,
            status="SUCCESS"
        )

        return  

    #6.  validate schema
    try:
        df = validate_schema(file_path)

        logger.info(
            "Schema validation successful | file='%s'",
            file_name,
        )

    except Exception as e:
        logger.exception(
            "Schema validation failed for file | file='%s' | error='%s'",
            file_name,
            str(e)
        )
        raise

    # 7. Run quality checks
    try:
        valid_df, invalid_df, quality_report = run_quality_checks(df)

        logger.info(
            "Quality checks completed | file='%s' |total=%d | valid=%d | invalid=%d",
            file_name,
            quality_report["total_rows"],
            quality_report["valid_rows"],
            quality_report["invalid_rows"]
        )

    except Exception as e:
        logger.exception(
            "Quality checks failed for file | file='%s' | error='%s'",
            file_name,
            str(e)
        )
        raise


    #8.  Quarantine invalid records
    if not invalid_df.empty:

        invalid_df = add_validation_errors(
            invalid_df,
            source_file=file_name,
        )

        logger.warning(
            "Invalid records found | file='%s' | count=%d",
            file_name,
            len(invalid_df)
        )

        rejected_file = Path(
            "data/rejected/"
            f"{file_name.replace('.parquet', '_invalid.parquet')}"
        ) 

        write_rejected_file(
            invalid_df,
            rejected_file
        )

        rejected_object_key = (
            f"taxi/ingestion_date={ingestion_date}/{rejected_file.name}"
        )

        upload_file(
            s3_client,
            REJECTED_BUCKET,
            rejected_object_key,
            str(rejected_file)
        )

        logger.info(
            "Rejected records uploaded | s3://%s/%s | count=%d",
            REJECTED_BUCKET,
            rejected_object_key,
            len(invalid_df)
        )
        
    # 9. Upload file to S3
    try:
        upload_file(
            s3_client,
            BRONZE_BUCKET,
            object_key,
            file_path
        )

        logger.info(
                "File uploaded successfully | s3://%s/%s.",
                BRONZE_BUCKET,
                object_key
            )


        # 10 Write ingestion metadata
        write_ingestion_matadata(
            file_path=file_path,
            ingestion_timestamp=ingestion_timestamp,
            file_size=file_size,
            target_bucket=BRONZE_BUCKET,
            target_key=object_key,
            status="SUCCESS"
        )

    except Exception as e:
        logger.exception(
            "Failed to upload file | file='%s'",
            file_name
        )
        raise

  

if __name__ == "__main__":
    # Example usage
    test_file_path = "data/source/yellow_tripdata_2026-05.parquet" 
    ingest(str(test_file_path))
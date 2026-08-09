from datetime import datetime, timezone
from pathlib import Path
import os
import boto3
from botocore.client import Config
from dotenv import load_dotenv
import logging

from .s3_client import (
    get_s3_client,
    object_exists,
    upload_file,
)

from .config import (
    BRONZE_BUCKET
)

from .metadata import write_ingestion_matadata

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger= logging.getLogger(__name__)

# load variables from .env
# load_dotenv()

# MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
# MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
# MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
# BRONZE_BUCKET = os.getenv("BRONZE_BUCKET")


def ingest(file_path:str):
    """
    Ingest a file to the S3 bucket.
    
    Args:
        file_path (str): The path to the file to be ingested.
    """
    file = Path(file_path)

    # 1. validate source file
    if not file.exists():
        logger.error("File does not exist: %s", file_path)
        raise FileNotFoundError(file_path)

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

    # 5. Check if object already exists in S3

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
            status="success"
        )

        return  

    # 6 Upload file to S3
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


        # 7 Write ingestion metadata
        write_ingestion_matadata(
            file_path=file_path,
            ingestion_timestamp=ingestion_timestamp,
            file_size=file_size,
            target_bucket=BRONZE_BUCKET,
            target_key=object_key,
            status="success"
        )

    except Exception as e:
        logger.exception(
            "Failed to upload file | file='%s'",
            file_name
        )
        raise

  

if __name__ == "__main__":
    # Example usage
    test_file_path = Path("data/source/yellow_tripdata_2026-05.parquet")  
    ingest(test_file_path)
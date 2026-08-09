from datetime import datetime, timezone
from pathlib import Path
import os
import boto3
from botocore.client import Config
from dotenv import load_dotenv
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger= logging.getLogger(__name__)

# load variables from .env
load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET")

def get_s3_client():
    """
    Create and return an S3 client for MinIO.
    """
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4')
    )

def upload_to_bronze(file_path: Path):
    """
    Upload a file to the specified S3 bucket.
    
    Args:
        file_path (Path): The path to the file to be uploaded.
    """
    s3_client = get_s3_client()
    ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    file = Path(file_path)

    if not file.exists():
        logger.error("File does not exist: %s", file_path)
        raise FileNotFoundError(file_path)
    
    file_name = file.name

    object_key = f"taxi/ingestion_date={ingestion_date}/{file_name}"
    file_size = file.stat().st_size

    logger.info(
        "Starting upload of file | file='%s' | size: %d bytes ",
        file_name,
        file_size
    )


    if object_exists(s3_client, BRONZE_BUCKET, object_key):
        # print(f"File '{file_name}' already exists in bucket '{BRONZE_BUCKET}' with key '{object_key}'. Skipping upload.")
        
        logger.info(
            "File already exists. Skipping upload | s3://%s/%s.",
            BRONZE_BUCKET,
            object_key
        )

        return

    try:
        s3_client.upload_file(
            str(file_path),
            BRONZE_BUCKET,
            object_key
        )

        logger.info(
                "File uploaded successfully | //%s/%s.",
                BRONZE_BUCKET,
                object_key
            )
    except Exception as e:
        logger.exception(
            "Failed to upload file | file='%s'",
            file_name
        )

        raise

    

def object_exists(s3,bucket:str, object_key: str)->bool:
    try:
        s3.head_object(
            Bucket=bucket, 
            Key=object_key
        )
        return True
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise



if __name__ == "__main__":
    # Example usage
    test_file_path = Path("data/source/yellow_tripdata_2026-05.parquet")  
    upload_to_bronze(test_file_path)
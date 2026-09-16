import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from .config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
)


def get_s3_client():
    """
    Create and return an S3 client configured for MinIO.

    Uses boto3 with S3V4 signature — required by MinIO.
    Credentials are loaded from environment variables via config.py.
    """
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )


def object_exists(s3, bucket: str, object_key: str) -> bool:
    """
    Check whether an object exists in the given S3/MinIO bucket.

    Uses head_object — cheap metadata-only request, no data transfer.

    Args:
        s3:         boto3 S3 client.
        bucket:     Bucket name.
        object_key: S3 object key to check.

    Returns:
        True if the object exists, False if it does not.

    Raises:
        ClientError: For any error other than 404 (e.g. permission denied).
    """
    try:
        s3.head_object(Bucket=bucket, Key=object_key)
        return True
    except ClientError as e:
        # 404 = object does not exist — expected for first-time ingestion
        if e.response["Error"]["Code"] == "404":
            return False
        # Any other error (403 permission denied, 503 service unavailable, etc.)
        # is re-raised so the pipeline fails loudly rather than silently skipping
        raise


def upload_file(s3_client, bucket: str, object_key: str, file_path: str) -> None:
    """
    Upload a local file to the specified S3/MinIO bucket.

    Args:
        s3_client:  boto3 S3 client.
        bucket:     Destination bucket name.
        object_key: S3 key (path) for the uploaded object.
        file_path:  Local filesystem path to the file.
    """
    s3_client.upload_file(str(file_path), bucket, object_key)

import boto3
from botocore.client import Config

from .config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,  
    MINIO_SECRET_KEY
)

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

def upload_file(s3_client, bucket: str, object_key: str, file_path: str):
    """
    Upload a file to the specified S3 bucket.
    
    Args:
        s3_client: The S3 client instance.
        bucket (str): The name of the S3 bucket.
        object_key (str): The key (path) for the uploaded object in the bucket.
        file_path (str): The local path to the file to be uploaded.
    """
    s3_client.upload_file(
        str(file_path),
        bucket,
        object_key
    )
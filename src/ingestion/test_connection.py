"""
MinIO connectivity smoke test.

Run this to verify your MinIO container is up and credentials are correct:
    python -m src.ingestion.test_connection
"""
import os

import boto3
from botocore.client import Config
from dotenv import load_dotenv

load_dotenv()


def main() -> None:
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
        config=Config(signature_version="s3v4"),
    )

    response = s3.list_buckets()
    buckets = response.get("Buckets", [])

    if not buckets:
        print("No buckets found. Create them in the MinIO console at http://localhost:9001")
        return

    print(f"Connected to MinIO. Found {len(buckets)} bucket(s):")
    for bucket in buckets:
        print(f"  - {bucket['Name']}")


if __name__ == "__main__":
    main()

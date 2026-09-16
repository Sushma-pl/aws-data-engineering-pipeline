import pandas as pd
from pathlib import Path

from .config import METADATA_PATH


def write_ingestion_metadata(
    file_path: str,
    ingestion_timestamp: str,
    file_size: int,
    target_bucket: str,
    target_key: str,
    status: str,
) -> None:
    """
    Append an ingestion event record to the metadata log.

    Idempotent: if a record with the same source_file AND status already
    exists, the write is skipped to prevent duplicates on re-runs.

    Args:
        file_path:            Local path to the source file.
        ingestion_timestamp:  Full ISO-8601 UTC timestamp (NOT truncated).
        file_size:            File size in bytes.
        target_bucket:        S3/MinIO bucket where the file was written.
        target_key:           S3 object key of the written file.
        status:               "SUCCESS" or "FAILED".
    """
    new_record = {
        "source_file":         Path(file_path).name,
        "source_path":         str(file_path),
        "file_size_in_bytes":  file_size,
        "ingestion_timestamp": ingestion_timestamp,   # full ISO string, not [:10]
        "target_bucket":       target_bucket,
        "target_key":          target_key,
        "status":              status,
    }

    new_df = pd.DataFrame([new_record])

    if Path(METADATA_PATH).exists():
        existing = pd.read_parquet(METADATA_PATH)

        already_logged = (
            (existing["source_file"] == new_record["source_file"]) &
            (existing["status"]      == new_record["status"])
        ).any()

        if already_logged:
            return  # idempotent: do not create a duplicate row

        updated = pd.concat([existing, new_df], ignore_index=True)
    else:
        updated = new_df

    Path(METADATA_PATH).parent.mkdir(parents=True, exist_ok=True)
    updated.to_parquet(METADATA_PATH, index=False)

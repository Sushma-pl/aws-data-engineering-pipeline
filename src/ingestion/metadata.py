import pandas as pd
from pathlib import Path

from .config import METADATA_PATH

def write_ingestion_matadata(
        file_path: str,
        ingestion_timestamp: str,
        file_size: int,
        target_bucket: str,
        target_key:str,
        status:str,
):

    metadata_file = {
        "source_file": Path(file_path).name,
        "source_path": str(file_path),
        "file_size_in_bytes": file_size,
        "ingestion_timestamp": ingestion_timestamp[:10],
        "target_bucket": target_bucket,
        "target_key": target_key,
        "status": status
    }

    new_record = pd.DataFrame([metadata_file])

    if Path(METADATA_PATH).exists():
        existing_metadata = pd.read_parquet(METADATA_PATH)
        already_processed =(
            (existing_metadata['source_file'] == metadata_file['source_file']) &
            (existing_metadata['status'] == metadata_file['status'])
        ).any()

        if already_processed:
            return
        
        updated_metadata = pd.concat(
            [existing_metadata, new_record],
            ignore_index=True
        )

    else:
        updated_metadata = new_record

    updated_metadata.to_parquet(
        METADATA_PATH, 
        index=False
    )
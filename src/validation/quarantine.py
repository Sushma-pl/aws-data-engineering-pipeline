from datetime import datetime, timezone
import pandas as pd
from pathlib import Path

def add_validation_errors(
        df: pd.DataFrame,
        source_file: str
) -> pd.DataFrame:
    # Implementation for adding validation errors
    
    result_df = df.copy()
    errors = []

    for index , row in result_df.iterrows():
        row_errors = []

        if pd.notna(row['trip_distance']) and row['trip_distance'] < 0:
            row_errors.append("Negative trip distance")

        if pd.notna(row['fare_amount']) and row['fare_amount'] < 0:
            row_errors.append("Negative fare amount")

        if pd.notna(row['total_amount']) and row['total_amount'] < 0:
            row_errors.append("Negative total amount")

        if  (
            pd.notna(row["tpep_pickup_datetime"]) and
            pd.notna(row["tpep_dropoff_datetime"]) and
            row["tpep_pickup_datetime"] > row["tpep_dropoff_datetime"]
        ):
            row_errors.append("Pickup datetime is after dropoff datetime")

        if pd.notna(row['passenger_count']) and row['passenger_count'] <= 0:
            row_errors.append("Invalid passenger count")

        if pd.notna(row['PULocationID']) and row['PULocationID'] < 0:
            row_errors.append("Invalid PULocationID")

        if pd.notna(row['DOLocationID']) and row['DOLocationID'] < 0:
            row_errors.append("Invalid DOLocationID")

        if pd.isna(row['VendorID']):
            row_errors.append("Null VendorID")

        if pd.isna(row['tpep_pickup_datetime']):
            row_errors.append("Null pickup datetime")

        if pd.isna(row['tpep_dropoff_datetime']):
            row_errors.append("Null dropoff datetime")

        if pd.isna(row['PULocationID']):
            row_errors.append("Null PULocationID")

        if pd.isna(row['DOLocationID']):
            row_errors.append("Null DOLocationID")

        if pd.isna(row['fare_amount']):
            row_errors.append("Null fare amount")

        if pd.isna(row['total_amount']):
            row_errors.append("Null total amount")

        errors.append(", ".join(row_errors) if row_errors else None)

    result_df['validation_errors'] = errors
    result_df['source_file'] = source_file
    result_df['rejection_timestamp'] = datetime.now(timezone.utc).isoformat()

    return result_df

def write_rejected_file(
    df: pd.DataFrame,
    output_path: str
)-> Path:
    """
    Write the rejected records to a Parquet file.

    Args:
        df (pd.DataFrame): DataFrame containing the rejected records.
        output_path (str): Path to save the rejected Parquet file.

    Returns:
        Path: The path to the saved rejected Parquet file.
    """

    output_file = Path(output_path)
    output_file.parent.mkdir(
        parents=True, 
        exist_ok=True
    )
    
    df.to_parquet(output_file, index=False)
    return output_file
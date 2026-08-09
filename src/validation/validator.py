from pathlib import Path
import pandas as pd

REQUIRED_COLUMNS = {
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee",
    "cbd_congestion_fee",
}

def validate_file(file_path: str) -> Path:
    """
    Validate the source file for required columns.
    
    Args:
        file_path (str): The path to the source file.
    
    Returns:
        Path: The path to the validated file.
    """

    file = Path(file_path)

    #1. check file existence
    if not file.exists():
        raise FileNotFoundError(f"File does not exist: {file_path}")

    #2. file format
    if file.suffix.lower() != ".parquet":
        raise ValueError(
            f"Unsupported file format: {file.suffix}. Only .parquet files are supported."
        )

    # 3. file size
    file_size = file.stat().st_size
    if file_size == 0:
        raise ValueError(f"File is empty: {file_path}")

    return file
    

def validate_schema(file_path: str) -> pd.DataFrame:
    """
    Validate the schema of the source file against the expected schema.
    
    Args:
        file_path (str): The path to the source file.
        
    Returns:
        Path: The path to the validated file.
    """

    # Read the parquet file into a DataFrame
    # 4. Read the parquet file into a DataFrame
    file = Path(file_path)

    try:
        df = pd.read_parquet(file_path)

    except Exception as e:
        raise ValueError(
            f"Error reading parquet file: {e}"
        )

    # 5. empty dataset
    if df.empty:
        raise ValueError(f"DataFrame is empty: {file_path}")

    # 6. Check for required columns
    missing_columns = REQUIRED_COLUMNS - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {', '.join(missing_columns)}"
        )

    return df


    
    
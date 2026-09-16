import pandas as pd


def run_quality_checks(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Run row-level data quality checks.

    Hard validation failures are separated into invalid_df.
    Warning-level checks are reported but do not invalidate rows.

    Args:
        df: DataFrame to validate.

    Returns:
        valid_df: Rows that pass all hard validation rules.
        invalid_df: Rows that fail one or more hard validation rules.
        quality_report: Dictionary containing validation metrics.
    """

    # ---------------------------------------------------------
    # 1. NULL CHECKS
    # ---------------------------------------------------------

    null_checks = {
        "VendorID": df["VendorID"].isna(),
        "pickup_datetime": df["tpep_pickup_datetime"].isna(),
        "dropoff_datetime": df["tpep_dropoff_datetime"].isna(),
        "PULocationID": df["PULocationID"].isna(),
        "DOLocationID": df["DOLocationID"].isna(),
        "fare_amount": df["fare_amount"].isna(),
        "total_amount": df["total_amount"].isna(),
    }

    # passenger_count NULL is a warning, not a hard failure
    passenger_nulls = df["passenger_count"].isna()

    # ---------------------------------------------------------
    # 2. NUMERIC / BUSINESS RULE CHECKS
    # ---------------------------------------------------------

    negative_trip_distance = df["trip_distance"] < 0
    negative_fare = df["fare_amount"] < 0
    negative_total = df["total_amount"] < 0
    invalid_passenger_count = df["passenger_count"] <= 0

    # ---------------------------------------------------------
    # 3. DATETIME CHECKS
    # ---------------------------------------------------------

    pickup_col  = pd.to_datetime(df["tpep_pickup_datetime"],  errors="coerce")
    dropoff_col = pd.to_datetime(df["tpep_dropoff_datetime"], errors="coerce")

    pickup_after_dropoff = pickup_col > dropoff_col

    duration_minutes = (dropoff_col - pickup_col).dt.total_seconds() / 60

    zero_duration = duration_minutes == 0
    long_duration = duration_minutes > 1440

    # ---------------------------------------------------------
    # 4. LOCATION CHECKS
    # ---------------------------------------------------------

    invalid_pu_location = df["PULocationID"] <= 0
    invalid_do_location = df["DOLocationID"] <= 0

    # ---------------------------------------------------------
    # 5. WARNING CHECKS
    # ---------------------------------------------------------

    zero_trip_distance = df["trip_distance"] == 0

    # ---------------------------------------------------------
    # 6. HARD INVALID MASK
    # ---------------------------------------------------------

    invalid_mask = (
        negative_trip_distance
        | negative_fare
        | negative_total
        | invalid_passenger_count
        | pickup_after_dropoff
        | invalid_pu_location
        | invalid_do_location
        | null_checks["VendorID"]
        | null_checks["pickup_datetime"]
        | null_checks["dropoff_datetime"]
        | null_checks["PULocationID"]
        | null_checks["DOLocationID"]
        | null_checks["fare_amount"]
        | null_checks["total_amount"]
    )

    invalid_df = df[invalid_mask].copy()
    valid_df = df[~invalid_mask].copy()

    # ---------------------------------------------------------
    # 7. QUALITY REPORT
    # ---------------------------------------------------------

    quality_report = {
        "total_rows": len(df),
        "valid_rows": len(valid_df),
        "invalid_rows": len(invalid_df),

        "hard_failures": {
            "null_VendorID": int(
                null_checks["VendorID"].sum()
            ),
            "null_pickup_datetime": int(
                null_checks["pickup_datetime"].sum()
            ),
            "null_dropoff_datetime": int(
                null_checks["dropoff_datetime"].sum()
            ),
            "null_PULocationID": int(
                null_checks["PULocationID"].sum()
            ),
            "null_DOLocationID": int(
                null_checks["DOLocationID"].sum()
            ),
            "null_fare_amount": int(
                null_checks["fare_amount"].sum()
            ),
            "null_total_amount": int(
                null_checks["total_amount"].sum()
            ),
            "negative_trip_distance": int(
                negative_trip_distance.sum()
            ),
            "negative_fare": int(
                negative_fare.sum()
            ),
            "negative_total": int(
                negative_total.sum()
            ),
            "invalid_passenger_count": int(
                invalid_passenger_count.sum()
            ),
            "pickup_after_dropoff": int(
                pickup_after_dropoff.sum()
            ),
            "invalid_PULocationID": int(
                invalid_pu_location.sum()
            ),
            "invalid_DOLocationID": int(
                invalid_do_location.sum()
            ),
        },

        "warnings": {
            "null_passenger_count": int(
                passenger_nulls.sum()
            ),
            "zero_trip_distance": int(
                zero_trip_distance.sum()
            ),
            "zero_duration": int(
                zero_duration.sum()
            ),
            "duration_over_24_hours": int(
                long_duration.sum()
            ),
        },
    }

    return valid_df, invalid_df, quality_report
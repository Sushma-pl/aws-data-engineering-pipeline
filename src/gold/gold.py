from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.ingestion.config import SILVER_BUCKET, GOLD_BUCKET


def read_silver(spark: SparkSession, silver_path: str) -> DataFrame:
    """Read all Silver Parquet data from the given path.

    Args:
        spark: Active SparkSession configured with S3A credentials.
        silver_path: Full S3A URI to the Silver dataset
                     (e.g. ``s3a://taxi-silver/taxi/``).

    Returns:
        A Spark DataFrame containing all Silver rows across every
        ``pickup_date`` partition found at *silver_path*.
    """
    return spark.read.parquet(silver_path)


def compute_daily_revenue(df: DataFrame) -> DataFrame:
    """Aggregate Silver rows into a per-day revenue summary.

    Groups by ``pickup_date`` and computes:
    - ``total_trip_count``  — ``count(*)``
    - ``total_fare_amount`` — ``sum(fare_amount)``
    - ``total_tip_amount``  — ``sum(tip_amount)``
    - ``total_amount``      — ``sum(total_amount)``
    - ``avg_fare_amount``   — ``avg(fare_amount)``

    Args:
        df: Silver DataFrame produced by ``read_silver``.

    Returns:
        A DataFrame with one row per distinct ``pickup_date`` and the
        five aggregate columns listed above.
    """
    return df.groupBy("pickup_date").agg(
        F.count("*").alias("total_trip_count"),
        F.sum("fare_amount").alias("total_fare_amount"),
        F.sum("tip_amount").alias("total_tip_amount"),
        F.sum("total_amount").alias("total_amount"),
        F.avg("fare_amount").alias("avg_fare_amount"),
    )


def compute_zone_metrics(df: DataFrame) -> DataFrame:
    """Aggregate Silver rows into per-day, per-pickup-zone metrics.

    Groups by (``pickup_date``, ``pickup_location_id``) and computes:
    - ``trip_count``               — ``count(*)``
    - ``avg_fare_amount``          — ``avg(fare_amount)``
    - ``avg_trip_distance``        — ``avg(trip_distance)``
    - ``avg_trip_duration_minutes``— ``avg(trip_duration_minutes)``

    Args:
        df: Silver DataFrame produced by ``read_silver``.

    Returns:
        A DataFrame with one row per distinct
        (``pickup_date``, ``pickup_location_id``) combination and the
        four aggregate columns listed above.
    """
    return df.groupBy("pickup_date", "pickup_location_id").agg(
        F.count("*").alias("trip_count"),
        F.avg("fare_amount").alias("avg_fare_amount"),
        F.avg("trip_distance").alias("avg_trip_distance"),
        F.avg("trip_duration_minutes").alias("avg_trip_duration_minutes"),
    )


def compute_hourly_demand(df: DataFrame) -> DataFrame:
    """Aggregate Silver rows into per-day, per-hour demand metrics.

    Filters to rows where ``pickup_hour`` is non-null and within [0, 23],
    then groups by (``pickup_date``, ``pickup_hour``) and computes:
    - ``trip_count``      — ``count(*)``
    - ``avg_fare_amount`` — ``avg(fare_amount)``

    Rows with a null or out-of-range ``pickup_hour`` are excluded from
    the output entirely.

    Args:
        df: Silver DataFrame produced by ``read_silver``.

    Returns:
        A DataFrame with one row per distinct
        (``pickup_date``, ``pickup_hour``) combination (where
        ``pickup_hour`` ∈ [0, 23]) and the two aggregate columns above.
    """
    return (
        df.filter(F.col("pickup_hour").isNotNull() & F.col("pickup_hour").between(0, 23))
        .groupBy("pickup_date", "pickup_hour")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("fare_amount").alias("avg_fare_amount"),
        )
    )


def write_gold(df: DataFrame, output_path: str, mode: str = "overwrite") -> None:
    """Write a Gold aggregation DataFrame to MinIO as Parquet.

    Args:
        df: Aggregated Gold DataFrame to persist.
        output_path: Full S3A URI for the destination
                     (e.g. ``s3a://taxi-gold/metrics/daily_revenue/``).
        mode: Spark write mode — ``"overwrite"`` (default) for daily
              revenue; ``"overwrite"`` with dynamic partition overwrite
              configured on the SparkSession for zone metrics and hourly
              demand.
    """
    df.write.mode(mode).partitionBy("pickup_date").parquet(output_path)


def run_gold_job(spark: SparkSession) -> None:
    """Orchestrate the full Gold aggregation pipeline.

    Reads Silver data, computes all three aggregations (daily revenue,
    zone metrics, hourly demand), and writes each result to its
    corresponding Gold path on MinIO:

    - ``s3a://{GOLD_BUCKET}/metrics/daily_revenue/``   — ``overwrite`` mode
    - ``s3a://{GOLD_BUCKET}/metrics/zone_metrics/``    — dynamic partition overwrite
    - ``s3a://{GOLD_BUCKET}/metrics/hourly_demand/``   — dynamic partition overwrite

    Args:
        spark: Active SparkSession with S3A credentials already configured.
               Dynamic-partition-overwrite mode is set on the session
               internally for zone metrics and hourly demand writes.
    """
    silver_path = f"s3a://{SILVER_BUCKET}/taxi/"

    daily_revenue_path = f"s3a://{GOLD_BUCKET}/metrics/daily_revenue/"
    zone_metrics_path = f"s3a://{GOLD_BUCKET}/metrics/zone_metrics/"
    hourly_demand_path = f"s3a://{GOLD_BUCKET}/metrics/hourly_demand/"

    # Read all Silver data
    silver_df = read_silver(spark, silver_path)

    # --- Daily revenue: full overwrite each run ---
    daily_revenue_df = compute_daily_revenue(silver_df)
    write_gold(daily_revenue_df, daily_revenue_path, mode="overwrite")
    print(f"Gold daily revenue written to: {daily_revenue_path}")

    # --- Zone metrics and hourly demand: dynamic partition overwrite ---
    # Only the pickup_date partitions present in the current Silver input
    # are replaced; older historical partitions are preserved.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    zone_metrics_df = compute_zone_metrics(silver_df)
    write_gold(zone_metrics_df, zone_metrics_path, mode="overwrite")
    print(f"Gold zone metrics written to: {zone_metrics_path}")

    hourly_demand_df = compute_hourly_demand(silver_df)
    write_gold(hourly_demand_df, hourly_demand_path, mode="overwrite")
    print(f"Gold hourly demand written to: {hourly_demand_path}")


if __name__ == "__main__":
    from src.ingestion.config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY

    spark = (
        SparkSession.builder
        .appName("TaxiSilverToGold")
        .master("local[*]")
        .config("spark.local.dir", "C:/spark-tmp")
        .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.2")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        run_gold_job(spark)
    finally:
        spark.stop()

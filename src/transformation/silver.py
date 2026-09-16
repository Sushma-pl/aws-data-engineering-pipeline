from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dotenv import load_dotenv
import os

from src.ingestion.config import (
    BRONZE_BUCKET,
    SILVER_BUCKET,
    REJECTED_BUCKET,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY
    
)

load_dotenv()


def create_spark_session() -> SparkSession:

    spark = (
        SparkSession.builder
        .appName("TaxiBronzeToSilver")
        .master("local[*]")
        .config(
            "spark.local.dir",
            "C:/spark-tmp"
        )
        .config(
            "spark.driver.extraJavaOptions", 
            "-Dfile.encoding=UTF-8"
        ) 
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.4.2"
        )
        .config(
            "spark.hadoop.fs.s3a.impl", 
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            MINIO_ENDPOINT
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            MINIO_ACCESS_KEY
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            MINIO_SECRET_KEY
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true"
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false"
        )
        .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    return spark


def read_bronze(
        spark: SparkSession,
        object_key: str
):
    bronze_path = f"s3a://{BRONZE_BUCKET}/{object_key}"
    return spark.read.parquet(bronze_path)


def transform_to_silver(df):
    transformed_df = (
        # standardize all column names
        df.withColumnRenamed("VendorID", "vendor_id")
        .withColumnRenamed("RatecodeID", "ratecode_id")
        .withColumnRenamed("PULocationID", "pickup_location_id")
        .withColumnRenamed("DOLocationID", "dropoff_location_id")
        .withColumnRenamed("Airport_fee", "airport_fee")

        # calculate trip duration
        .withColumn(
            "trip_duration_minutes", 
            (F.unix_timestamp("tpep_dropoff_datetime")-F.unix_timestamp("tpep_pickup_datetime"))/60
        )

        .withColumn(
            "pickup_date", 
            F.to_date("tpep_pickup_datetime")
        )

        .withColumn(
                "pickup_hour", 
                F.hour("tpep_pickup_datetime")
        )
    )

    rejection_reasons = F.array(
        F.when(
            F.col("vendor_id").isNull(),
            F.lit("vendor_id is null")
        ),
        F.when(
            F.col("tpep_pickup_datetime").isNull(),
            F.lit("pickup datetime is null")
        ),
        F.when(
            F.col("tpep_dropoff_datetime").isNull(),
            F.lit("dropoff datetime is null")
        ),
        F.when(
            F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime"),
            F.lit("dropoff datetime is before pickup datetime")
        ),
        F.when(
            F.col("trip_distance") < 0,
            F.lit("trip_distance is negative")
        ),
        F.when(
            F.col("fare_amount").isNull(),
            F.lit("fare_amount is null")
        ),
        F.when(
            F.col("fare_amount") < 0,
            F.lit("fare_amount is negative")
        ),
        F.when(
            F.col("total_amount").isNull(),
            F.lit("total_amount is null")
        ),
        F.when(
            F.col("total_amount") < 0,
            F.lit("total_amount is negative")
        ),
        F.when(
            F.col("passenger_count") <= 0,
            F.lit("passenger_count must be greater than zero")
        ),
        F.when(
            F.col("pickup_location_id").isNull(),
            F.lit("pickup_location_id is null")
        ),
        F.when(
            F.col("pickup_location_id") <= 0,
            F.lit("pickup_location_id must be greater than zero")
        ),
        F.when(
            F.col("dropoff_location_id").isNull(),
            F.lit("dropoff_location_id is null")
        ),
        F.when(
            F.col("dropoff_location_id") <= 0,
            F.lit("dropoff_location_id must be greater than zero")
        ),
    )

    with_rejections_df = transformed_df.withColumn(
        "rejection_reason",
        F.concat_ws(", ", F.array_compact(rejection_reasons))
    )

    silver_df = with_rejections_df.filter(
        F.col("rejection_reason") == ""
    ).drop("rejection_reason")

    rejected_df = with_rejections_df.filter(
        F.col("rejection_reason") != ""
    ).withColumn(
        "rejection_stage",
        F.lit("silver")
    ).withColumn(
        "rejection_timestamp",
        F.current_timestamp()
    )

    return silver_df, rejected_df


def write_silver(df):
    silver_path = f"s3a://{SILVER_BUCKET}/taxi"

    (
        df.write
        .mode("overwrite")
        .partitionBy("pickup_date")
        .parquet(silver_path)
    )

    print(f"Silver data written to : {silver_path}")


def write_rejected(df):
    rejected_path = f"s3a://{REJECTED_BUCKET}/taxi/rejection_stage=silver"

    (
        df.write
        .mode("overwrite")
        .partitionBy("pickup_date")
        .parquet(rejected_path)
    )

    print(f"Rejected Silver data written to : {rejected_path}")


def inspect_dataframes(bronze_df, silver_df, rejected_df):

    bronze_count = bronze_df.count()
    silver_count = silver_df.count()
    rejected_count = rejected_df.count()

    rejection_percentage = (
        rejected_count/bronze_count  *100 if bronze_count >0 else 0
    )

    print("\n========== DATA QUALITY SUMMARY ==========")

    print(f"Bronze rows       : {bronze_count:,}")
    print(f"Silver rows       : {silver_count:,}")
    print(f"Rejected rows     : {rejected_count:,}")
    print(f"Rejection rate    : {rejection_percentage:.2f}%")
    print(f"Row reconciliation: {bronze_count == silver_count + rejected_count}")
    print("==========================================\n")

    print("========== SILVER SCHEMA ==========")
    silver_df.printSchema()

    print("\n========== SILVER SAMPLE ==========")
    silver_df.show(5, truncate=False)

    print("\n========== REJECTED SAMPLE ==========")
    rejected_df.select("rejection_reason").show(10, truncate=False)


if __name__ == "__main__":

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    object_key = (
        "taxi/"
        "ingestion_date=2026-08-09/"
        "yellow_tripdata_2026-05.parquet"
    )

    bronze_df = read_bronze(spark, object_key=object_key)
    silver_df, rejected_df = transform_to_silver(bronze_df)

    inspect_dataframes(bronze_df, silver_df, rejected_df)
    write_silver(silver_df)
    write_rejected(rejected_df)
    spark.stop()

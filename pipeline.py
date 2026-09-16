"""
pipeline.py — CLI entrypoint for the NYC Yellow Taxi data engineering pipeline.

Drives Bronze ingestion → Silver transformation → Gold aggregation in sequence.
Usage:
    python pipeline.py --source-file data/source/yellow_tripdata_2026-05.parquet
"""

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from src.ingestion.ingest import ingest
from src.transformation.silver import transform_to_silver, write_silver, read_bronze

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """
    Parse and validate CLI arguments.

    Returns:
        argparse.Namespace with attribute `source_file` (str).

    Exits:
        Non-zero exit if `--source-file` is absent, points to a non-existent
        path, or the file is not readable.
    """
    parser = argparse.ArgumentParser(
        description="Run the NYC Yellow Taxi data engineering pipeline.",
    )
    parser.add_argument(
        "--source-file",
        required=True,
        metavar="PATH",
        help="Path to the source Parquet file to ingest.",
    )

    args = parser.parse_args()

    # Validate that the path resolves to an existing, readable file
    source_path = Path(args.source_file)

    if not source_path.exists():
        print(
            f"Error: --source-file '{args.source_file}' does not exist.",
            file=sys.stderr,
        )
        sys.exit(1)

    if not source_path.is_file():
        print(
            f"Error: --source-file '{args.source_file}' is not a file.",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        # Check readability by opening briefly
        with open(source_path, "rb"):
            pass
    except PermissionError:
        print(
            f"Error: --source-file '{args.source_file}' is not readable (permission denied).",
            file=sys.stderr,
        )
        sys.exit(1)

    return args


def run_pipeline(source_file: str) -> int:
    """
    Execute Bronze → Silver → Gold pipeline in sequence.

    Args:
        source_file: Local path to the source Parquet file.

    Returns:
        0 on success, 1 on failure.
    """
    from src.transformation.silver import create_spark_session
    from src.validation.quality_checks import run_quality_checks
    from src.validation.validator import validate_schema
    from src.gold.gold import run_gold_job

    file_name = Path(source_file).name
    spark = None

    # ── Pre-flight: read row counts for the final summary log ─────────────────
    # This is a lightweight pandas read; it does NOT count as a pipeline stage.
    total_rows = valid_rows = rejected_rows = 0
    try:
        df_for_counts = validate_schema(source_file)
        _, _, quality_report = run_quality_checks(df_for_counts)
        total_rows = quality_report["total_rows"]
        valid_rows = quality_report["valid_rows"]
        rejected_rows = quality_report["invalid_rows"]
    except Exception as e:
        # Non-fatal — counts default to 0; Bronze stage will surface the real error.
        logger.warning("Could not pre-compute row counts for summary: %s", e)

    # ── Stage 1: Bronze ingestion ─────────────────────────────────────────────
    try:
        ingest(source_file)
        logger.info("Bronze ingestion completed | file=%s", file_name)
    except Exception as e:
        logger.error("Bronze ingestion failed | file=%s | reason=%s", file_name, e)
        return 1

    # ── Stages 2 & 3: Silver + Gold (share one SparkSession) ─────────────────
    try:
        spark = create_spark_session()

        # Stage 2: Silver transformation
        try:
            # Reconstruct the object key that ingest() used when uploading to Bronze.
            ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            bronze_object_key = f"taxi/ingestion_date={ingestion_date}/{file_name}"

            bronze_df = read_bronze(spark, bronze_object_key)
            silver_df, rejected_silver_df = transform_to_silver(bronze_df)
            write_silver(silver_df)
            logger.info("Silver transformation completed | file=%s", file_name)
        except Exception as e:
            logger.error(
                "Silver transformation failed | file=%s | reason=%s", file_name, e
            )
            return 1

        # Stage 3: Gold aggregation
        try:
            run_gold_job(spark)
            logger.info("Gold aggregation completed | file=%s", file_name)
        except Exception as e:
            logger.error(
                "Gold aggregation failed | file=%s | reason=%s", file_name, e
            )
            return 1

    finally:
        if spark is not None:
            spark.stop()

    # ── Success summary ───────────────────────────────────────────────────────
    rejection_rate = (
        round((rejected_rows / total_rows) * 100, 2) if total_rows > 0 else 0.00
    )
    logger.info(
        "Pipeline complete | file=%s | total_rows=%d | valid_rows=%d"
        " | rejected_rows=%d | rejection_rate=%.2f%%",
        file_name,
        total_rows,
        valid_rows,
        rejected_rows,
        rejection_rate,
    )
    return 0


def main() -> None:
    """
    CLI entry point: parse args, run pipeline, exit with the returned code.
    """
    args = parse_args()
    exit_code = run_pipeline(args.source_file)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()

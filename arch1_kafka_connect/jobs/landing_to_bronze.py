"""
Architecture 1 -- Phase 1: Landing → Bronze

Reads Parquet files written by Kafka Connect S3 Sink from MinIO (S3A),
adds metadata columns, and writes a partitioned Delta table to the
bronze layer on the local filesystem.

Usage (inside the Spark container):
    spark-submit --master spark://spark-master:7077 /jobs/arch1/landing_to_bronze.py
"""

import uuid
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
S3A_INPUT_PATH = "s3a://lakehouse/topics/orders.raw/**/*.parquet"
BRONZE_OUTPUT_PATH = "/data/arch1/bronze/orders"
BATCH_ID = str(uuid.uuid4())


def create_spark_session() -> SparkSession:
    """Create a SparkSession with Delta Lake and S3A support."""
    return (
        SparkSession.builder
        .appName("arch1-landing-to-bronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # S3A / MinIO connectivity
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Delta optimizations
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .getOrCreate()
    )


def read_landing_parquet(spark: SparkSession):
    """Read all Parquet files from the MinIO landing zone."""
    logger.info("Reading Parquet files from: %s", S3A_INPUT_PATH)
    df = spark.read.parquet(S3A_INPUT_PATH)
    logger.info("Landing schema: %s", df.schema.simpleString())
    logger.info("Landing row count: %d", df.count())
    return df


def add_bronze_metadata(df):
    """Add bronze-layer metadata columns."""
    return df.withColumn(
        "_ingested_at", F.current_timestamp()
    ).withColumn(
        "_source_file", F.input_file_name()
    ).withColumn(
        "_batch_id", F.lit(BATCH_ID)
    ).withColumn(
        "ingestion_date", F.to_date(F.col("_ingested_at"))
    )


def write_bronze_delta(df) -> None:
    """Append enriched records to the Bronze Delta table."""
    logger.info("Writing Bronze Delta table to: %s", BRONZE_OUTPUT_PATH)
    (
        df.write
        .format("delta")
        .mode("append")
        .partitionBy("ingestion_date")
        .save(BRONZE_OUTPUT_PATH)
    )
    logger.info("Bronze write complete.")


def optimize_bronze(spark: SparkSession) -> None:
    """Run Delta OPTIMIZE to compact small files in the bronze table."""
    logger.info("Running OPTIMIZE on Bronze table...")
    spark.sql(f"OPTIMIZE delta.`{BRONZE_OUTPUT_PATH}`")
    logger.info("OPTIMIZE complete.")


def main():
    logger.info("=== Landing to Bronze (Architecture 1) ===")
    logger.info("Batch ID: %s", BATCH_ID)

    spark = create_spark_session()

    try:
        landing_df = read_landing_parquet(spark)
        bronze_df = add_bronze_metadata(landing_df)
        write_bronze_delta(bronze_df)
        optimize_bronze(spark)
        logger.info("Job finished successfully.")
    except Exception:
        logger.exception("Job failed with an unexpected error.")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

"""
Architecture 1 -- Phase 2: Bronze → Silver

Reads the Bronze Delta table, applies type casts, validates data quality
constraints, deduplicates orders (latest record per order_id), and
upserts the result into the Silver Delta table using a MERGE operation.

Usage (inside the Spark container):
    spark-submit --master spark://spark-master:7077 /jobs/arch1/bronze_to_silver.py
"""

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, DateType
from pyspark.sql.window import Window
from delta.tables import DeltaTable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BRONZE_PATH = "/data/arch1/bronze/orders"
SILVER_PATH = "/data/arch1/silver/orders"


def create_spark_session() -> SparkSession:
    """Create a SparkSession with Delta Lake support."""
    return (
        SparkSession.builder
        .appName("arch1-bronze-to-silver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .getOrCreate()
    )


def read_bronze(spark: SparkSession) -> DataFrame:
    """Read the Bronze Delta table."""
    logger.info("Reading Bronze Delta table from: %s", BRONZE_PATH)
    df = spark.read.format("delta").load(BRONZE_PATH)
    logger.info("Bronze row count (before filtering): %d", df.count())
    return df


def cast_and_derive(df: DataFrame) -> DataFrame:
    """
    Apply type casts and derive computed columns.

    - order_timestamp: long (epoch milliseconds) → TimestampType
    - order_date: derived from order_timestamp (DateType), used as partition key
    """
    return (
        df
        # Convert epoch-millisecond long to Spark TimestampType
        .withColumn(
            "order_timestamp",
            (F.col("order_timestamp") / 1000).cast(TimestampType()),
        )
        # Derive calendar date from the cast timestamp
        .withColumn(
            "order_date",
            F.col("order_timestamp").cast(DateType()),
        )
    )


def validate(df: DataFrame) -> DataFrame:
    """
    Filter out records that violate data quality constraints:
    - total_amount must be > 0
    - quantity must be > 0
    """
    before = df.count()
    valid_df = df.filter(
        (F.col("total_amount") > 0) & (F.col("quantity") > 0)
    )
    after = valid_df.count()
    dropped = before - after
    if dropped > 0:
        logger.warning("Dropped %d invalid records (total_amount <= 0 or quantity <= 0).", dropped)
    else:
        logger.info("All %d records passed validation.", after)
    return valid_df


def deduplicate(df: DataFrame) -> DataFrame:
    """
    Deduplicate by order_id, retaining the record with the latest
    order_timestamp (using a Window rank).
    """
    window_spec = Window.partitionBy("order_id").orderBy(F.col("order_timestamp").desc())
    deduped = (
        df
        .withColumn("_rank", F.row_number().over(window_spec))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )
    before = df.count()
    after = deduped.count()
    logger.info("Deduplication: %d → %d rows (removed %d duplicates).", before, after, before - after)
    return deduped


def select_silver_columns(df: DataFrame) -> DataFrame:
    """Select and order columns for the Silver layer."""
    return df.select(
        "order_id",
        "customer_id",
        "customer_name",
        "customer_email",
        "order_status",
        "product_id",
        "product_name",
        "product_category",
        "quantity",
        "unit_price",
        "total_amount",
        "currency",
        "order_timestamp",
        "order_date",
        "shipping_address_city",
        "shipping_address_state",
        "shipping_address_country",
        "_ingested_at",
        "_source_file",
        "_batch_id",
    )


def upsert_to_silver(spark: SparkSession, silver_df: DataFrame) -> None:
    """
    Merge the prepared Silver DataFrame into the Silver Delta table.

    - If a matching order_id already exists and the incoming record is newer,
      update all columns.
    - If no matching record exists, insert the new row.
    """
    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        logger.info("Silver Delta table exists -- performing MERGE upsert.")
        silver_table = DeltaTable.forPath(spark, SILVER_PATH)

        (
            silver_table.alias("target")
            .merge(
                silver_df.alias("source"),
                "target.order_id = source.order_id",
            )
            .whenMatchedUpdate(
                condition="source.order_timestamp > target.order_timestamp",
                set={col: f"source.{col}" for col in silver_df.columns},
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info("MERGE complete.")
    else:
        logger.info("Silver Delta table does not exist -- creating with initial write.")
        (
            silver_df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("order_date")
            .save(SILVER_PATH)
        )
        logger.info("Initial Silver write complete.")


def main():
    logger.info("=== Bronze to Silver (Architecture 1) ===")
    spark = create_spark_session()

    try:
        bronze_df = read_bronze(spark)
        typed_df = cast_and_derive(bronze_df)
        valid_df = validate(typed_df)
        deduped_df = deduplicate(valid_df)
        silver_df = select_silver_columns(deduped_df)
        upsert_to_silver(spark, silver_df)
        logger.info("Job finished successfully.")
    except Exception:
        logger.exception("Job failed with an unexpected error.")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

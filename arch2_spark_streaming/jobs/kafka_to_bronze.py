"""
Architecture 2 -- Kafka → Bronze (PySpark Structured Streaming)

Consumes Avro-encoded messages from the Kafka topic `orders.raw`,
strips the 5-byte Confluent wire-format header (magic byte + schema-id),
deserializes the payload using `from_avro()`, adds metadata columns,
and writes a partitioned Delta table to the Bronze layer.

Usage (inside the Spark container):
    spark-submit --master spark://spark-master:7077 /jobs/arch2/kafka_to_bronze.py

The job runs indefinitely (streaming); terminate with SIGTERM / Ctrl-C.
"""

import os
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType, StringType, DateType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders.raw")
BRONZE_OUTPUT_PATH = "/data/arch2/bronze/orders"
CHECKPOINT_PATH = "/data/arch2/bronze/_checkpoint"
SCHEMA_PATH = os.getenv("AVRO_SCHEMA_PATH", "/schemas/order.avsc")
MAX_OFFSETS_PER_TRIGGER = 10_000


def load_avro_schema(path: str) -> str:
    """Load Avro schema JSON string from the .avsc file."""
    with open(path, "r") as f:
        return f.read()


def create_spark_session() -> SparkSession:
    """Create a SparkSession with Delta Lake and Kafka support."""
    return (
        SparkSession.builder
        .appName("arch2-kafka-to-bronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def strip_confluent_header_udf():
    """
    Returns a UDF that strips the 5-byte Confluent Avro wire-format header.

    Confluent wire format layout:
        Byte 0:    Magic byte (0x00)
        Bytes 1-4: Schema ID (big-endian int32)
        Bytes 5+:  Avro-encoded payload

    The native Spark `from_avro()` expects a raw Avro payload without this
    header, so we strip the first 5 bytes before passing the bytes to it.
    """
    def _strip(payload: bytes) -> bytes:
        if payload is None:
            return None
        if len(payload) < 5:
            return payload
        # Validate magic byte
        if payload[0] != 0x00:
            # Not a Confluent-encoded message; return as-is
            return payload
        return bytes(payload[5:])

    return F.udf(_strip, BinaryType())


def read_kafka_stream(spark: SparkSession):
    """Create the Kafka streaming DataFrame."""
    logger.info("Connecting to Kafka at %s, topic: %s", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", str(MAX_OFFSETS_PER_TRIGGER))
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_avro_payload(raw_df, avro_schema_str: str):
    """
    Strip the Confluent 5-byte header and deserialize Avro binary payload.

    Returns a DataFrame with the order fields flattened to top-level columns,
    plus Kafka metadata columns.
    """
    strip_udf = strip_confluent_header_udf()

    # Strip header to get raw Avro bytes
    stripped_df = raw_df.withColumn("avro_payload", strip_udf(F.col("value")))

    # Deserialize using Spark's built-in from_avro (no Schema Registry required)
    from pyspark.sql.avro.functions import from_avro

    parsed_df = stripped_df.withColumn(
        "order",
        from_avro(F.col("avro_payload"), avro_schema_str),
    )

    # Flatten nested struct fields to top-level columns
    order_fields = [
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
        "shipping_address_city",
        "shipping_address_state",
        "shipping_address_country",
    ]

    flattened = parsed_df.select(
        *[F.col(f"order.{field}").alias(field) for field in order_fields],
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").alias("_kafka_offset"),
        F.col("timestamp").alias("_kafka_timestamp"),
    )

    return flattened


def add_bronze_metadata(df):
    """Add bronze-layer metadata columns."""
    return (
        df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("ingestion_date", F.to_date(F.col("_ingested_at")))
    )


def write_stream_to_bronze(df):
    """
    Read all unread Kafka offsets (since last checkpoint) and write to Bronze Delta.

    Uses trigger(availableNow=True): processes all currently available data across
    as many micro-batches as needed, then stops. The checkpoint persists the last
    committed offset so each run is a CDC-style incremental load.
    """
    logger.info("Starting Bronze Delta write (availableNow) at: %s", BRONZE_OUTPUT_PATH)
    return (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .partitionBy("ingestion_date")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start(BRONZE_OUTPUT_PATH)
    )


def main():
    logger.info("=== Kafka to Bronze Streaming Job (Architecture 2) ===")
    logger.info("Schema path:     %s", SCHEMA_PATH)
    logger.info("Output path:     %s", BRONZE_OUTPUT_PATH)
    logger.info("Checkpoint path: %s", CHECKPOINT_PATH)
    logger.info("Trigger:         availableNow (CDC run-to-completion)")
    logger.info("Max offsets:     %d", MAX_OFFSETS_PER_TRIGGER)

    avro_schema_str = load_avro_schema(SCHEMA_PATH)
    logger.info("Loaded Avro schema from %s", SCHEMA_PATH)

    spark = create_spark_session()

    try:
        raw_df = read_kafka_stream(spark)
        parsed_df = parse_avro_payload(raw_df, avro_schema_str)
        bronze_df = add_bronze_metadata(parsed_df)
        query = write_stream_to_bronze(bronze_df)

        logger.info("Streaming query started. Processing all available offsets then stopping...")
        query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("Shutdown requested.")
    except Exception:
        logger.exception("Streaming job failed with an unexpected error.")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()

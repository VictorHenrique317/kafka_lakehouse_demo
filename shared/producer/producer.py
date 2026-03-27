"""
Infinite e-commerce order event producer.

Publishes Avro-serialized Order events to Kafka using the Confluent Schema Registry.
Configuration is driven entirely by environment variables.
"""

import os
import time
import uuid
import random
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from faker import Faker
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders.raw")
PRODUCER_INTERVAL_MS = int(os.getenv("PRODUCER_INTERVAL_MS", "100"))

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schemas", "order.avsc")

# ---------------------------------------------------------------------------
# Product catalogue (50 realistic products across 5 categories)
# ---------------------------------------------------------------------------
PRODUCTS = [
    # Electronics
    {"product_id": "ELEC-001", "product_name": "Wireless Noise-Cancelling Headphones", "product_category": "Electronics", "unit_price": 249.99},
    {"product_id": "ELEC-002", "product_name": "4K Smart TV 55\"", "product_category": "Electronics", "unit_price": 699.99},
    {"product_id": "ELEC-003", "product_name": "Mechanical Gaming Keyboard", "product_category": "Electronics", "unit_price": 129.99},
    {"product_id": "ELEC-004", "product_name": "USB-C Laptop Docking Station", "product_category": "Electronics", "unit_price": 89.99},
    {"product_id": "ELEC-005", "product_name": "Portable Bluetooth Speaker", "product_category": "Electronics", "unit_price": 79.99},
    {"product_id": "ELEC-006", "product_name": "Wireless Charging Pad", "product_category": "Electronics", "unit_price": 29.99},
    {"product_id": "ELEC-007", "product_name": "Smart Home Hub", "product_category": "Electronics", "unit_price": 149.99},
    {"product_id": "ELEC-008", "product_name": "Action Camera 4K", "product_category": "Electronics", "unit_price": 299.99},
    {"product_id": "ELEC-009", "product_name": "Ergonomic Wireless Mouse", "product_category": "Electronics", "unit_price": 59.99},
    {"product_id": "ELEC-010", "product_name": "External SSD 1TB", "product_category": "Electronics", "unit_price": 109.99},
    # Clothing
    {"product_id": "CLTH-001", "product_name": "Classic Denim Jacket", "product_category": "Clothing", "unit_price": 89.99},
    {"product_id": "CLTH-002", "product_name": "Running Sneakers Pro", "product_category": "Clothing", "unit_price": 119.99},
    {"product_id": "CLTH-003", "product_name": "Merino Wool Sweater", "product_category": "Clothing", "unit_price": 74.99},
    {"product_id": "CLTH-004", "product_name": "Waterproof Hiking Boots", "product_category": "Clothing", "unit_price": 159.99},
    {"product_id": "CLTH-005", "product_name": "Slim Fit Chino Pants", "product_category": "Clothing", "unit_price": 54.99},
    {"product_id": "CLTH-006", "product_name": "Fleece Zip Hoodie", "product_category": "Clothing", "unit_price": 64.99},
    {"product_id": "CLTH-007", "product_name": "Linen Summer Dress", "product_category": "Clothing", "unit_price": 49.99},
    {"product_id": "CLTH-008", "product_name": "Leather Belt Premium", "product_category": "Clothing", "unit_price": 34.99},
    {"product_id": "CLTH-009", "product_name": "Thermal Base Layer Set", "product_category": "Clothing", "unit_price": 44.99},
    {"product_id": "CLTH-010", "product_name": "Canvas Tote Bag", "product_category": "Clothing", "unit_price": 24.99},
    # Books
    {"product_id": "BOOK-001", "product_name": "Designing Data-Intensive Applications", "product_category": "Books", "unit_price": 49.99},
    {"product_id": "BOOK-002", "product_name": "Clean Code: A Handbook", "product_category": "Books", "unit_price": 39.99},
    {"product_id": "BOOK-003", "product_name": "The Pragmatic Programmer", "product_category": "Books", "unit_price": 44.99},
    {"product_id": "BOOK-004", "product_name": "Fundamentals of Data Engineering", "product_category": "Books", "unit_price": 54.99},
    {"product_id": "BOOK-005", "product_name": "Python Cookbook 3rd Edition", "product_category": "Books", "unit_price": 34.99},
    {"product_id": "BOOK-006", "product_name": "Kafka: The Definitive Guide", "product_category": "Books", "unit_price": 59.99},
    {"product_id": "BOOK-007", "product_name": "The Data Warehouse Toolkit", "product_category": "Books", "unit_price": 64.99},
    {"product_id": "BOOK-008", "product_name": "Learning Spark 2nd Edition", "product_category": "Books", "unit_price": 49.99},
    {"product_id": "BOOK-009", "product_name": "Site Reliability Engineering", "product_category": "Books", "unit_price": 44.99},
    {"product_id": "BOOK-010", "product_name": "System Design Interview Vol 2", "product_category": "Books", "unit_price": 29.99},
    # Home & Garden
    {"product_id": "HOME-001", "product_name": "Bamboo Cutting Board Set", "product_category": "Home & Garden", "unit_price": 34.99},
    {"product_id": "HOME-002", "product_name": "Cast Iron Dutch Oven 5.5qt", "product_category": "Home & Garden", "unit_price": 79.99},
    {"product_id": "HOME-003", "product_name": "Smart Thermostat WiFi", "product_category": "Home & Garden", "unit_price": 129.99},
    {"product_id": "HOME-004", "product_name": "Air Purifier HEPA H13", "product_category": "Home & Garden", "unit_price": 199.99},
    {"product_id": "HOME-005", "product_name": "Stainless Steel Water Bottle 32oz", "product_category": "Home & Garden", "unit_price": 29.99},
    {"product_id": "HOME-006", "product_name": "Robot Vacuum Cleaner", "product_category": "Home & Garden", "unit_price": 349.99},
    {"product_id": "HOME-007", "product_name": "Indoor Plant Grow Light", "product_category": "Home & Garden", "unit_price": 44.99},
    {"product_id": "HOME-008", "product_name": "French Press Coffee Maker", "product_category": "Home & Garden", "unit_price": 39.99},
    {"product_id": "HOME-009", "product_name": "Memory Foam Pillow Set (2)", "product_category": "Home & Garden", "unit_price": 54.99},
    {"product_id": "HOME-010", "product_name": "Herb Garden Starter Kit", "product_category": "Home & Garden", "unit_price": 24.99},
    # Sports
    {"product_id": "SPRT-001", "product_name": "Adjustable Dumbbell Set 5-50lb", "product_category": "Sports", "unit_price": 299.99},
    {"product_id": "SPRT-002", "product_name": "Yoga Mat Non-Slip 6mm", "product_category": "Sports", "unit_price": 39.99},
    {"product_id": "SPRT-003", "product_name": "Resistance Band Set (5 levels)", "product_category": "Sports", "unit_price": 24.99},
    {"product_id": "SPRT-004", "product_name": "Cycling Helmet MIPS", "product_category": "Sports", "unit_price": 89.99},
    {"product_id": "SPRT-005", "product_name": "GPS Running Watch", "product_category": "Sports", "unit_price": 199.99},
    {"product_id": "SPRT-006", "product_name": "Pull-Up Bar Doorway", "product_category": "Sports", "unit_price": 34.99},
    {"product_id": "SPRT-007", "product_name": "Foam Roller Deep Tissue", "product_category": "Sports", "unit_price": 29.99},
    {"product_id": "SPRT-008", "product_name": "Hiking Backpack 45L", "product_category": "Sports", "unit_price": 119.99},
    {"product_id": "SPRT-009", "product_name": "Tennis Racket Pro", "product_category": "Sports", "unit_price": 149.99},
    {"product_id": "SPRT-010", "product_name": "Jump Rope Speed Cable", "product_category": "Sports", "unit_price": 19.99},
]

ORDER_STATUSES = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]
CURRENCIES = ["USD", "EUR", "BRL"]

# Weight towards realistic status distribution
STATUS_WEIGHTS = [0.20, 0.30, 0.20, 0.25, 0.05]


def load_schema(path: str) -> str:
    """Load Avro schema string from file."""
    with open(path, "r") as f:
        return f.read()


def make_order(fake: Faker) -> dict:
    """Generate a single synthetic order event."""
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 5)
    unit_price = product["unit_price"]
    total_amount = round(quantity * unit_price, 2)
    order_status = random.choices(ORDER_STATUSES, weights=STATUS_WEIGHTS, k=1)[0]
    currency = random.choice(CURRENCIES)
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": str(uuid.uuid4()),
        "customer_name": fake.name(),
        "customer_email": fake.email(),
        "order_status": order_status,
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "product_category": product["product_category"],
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": total_amount,
        "currency": currency,
        "order_timestamp": now_ms,
        "shipping_address_city": fake.city(),
        "shipping_address_state": fake.state(),
        "shipping_address_country": fake.country(),
    }


def delivery_report(err, msg):
    """Kafka delivery callback."""
    if err is not None:
        logger.error("Delivery failed for key %s: %s", msg.key(), err)
    else:
        logger.debug(
            "Delivered to %s [%d] @ offset %d",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


def create_producer(schema_str: str) -> SerializingProducer:
    """Build and return a SerializingProducer with Avro value serializer."""
    schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer(
        schema_registry_client,
        schema_str,
        conf={"auto.register.schemas": True},
    )

    producer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": avro_serializer,
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
    }

    return SerializingProducer(producer_conf)


def main():
    logger.info("Starting order event producer")
    logger.info("  Kafka bootstrap: %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("  Schema Registry: %s", SCHEMA_REGISTRY_URL)
    logger.info("  Topic:           %s", KAFKA_TOPIC)
    logger.info("  Interval:        %d ms", PRODUCER_INTERVAL_MS)

    schema_str = load_schema(SCHEMA_PATH)
    fake = Faker()
    producer = create_producer(schema_str)

    interval_sec = PRODUCER_INTERVAL_MS / 1000.0
    produced_count = 0
    log_every = 500  # log progress every N messages

    logger.info("Producer ready. Entering infinite loop...")

    while True:
        try:
            order = make_order(fake)
            producer.produce(
                topic=KAFKA_TOPIC,
                key=order["order_id"],
                value=order,
                on_delivery=delivery_report,
            )
            # Poll to trigger delivery callbacks and handle backpressure
            producer.poll(0)

            produced_count += 1
            if produced_count % log_every == 0:
                logger.info(
                    "Produced %d messages. Last order_id=%s status=%s",
                    produced_count,
                    order["order_id"],
                    order["order_status"],
                )

            time.sleep(interval_sec)

        except BufferError:
            logger.warning("Producer queue full -- flushing before continuing")
            producer.flush()

        except KeyboardInterrupt:
            logger.info("Shutdown requested. Flushing remaining messages...")
            break

        except Exception as exc:  # noqa: BLE001
            logger.error("Unexpected error: %s", exc, exc_info=True)
            time.sleep(5)  # back off before retrying

    producer.flush()
    logger.info("Producer shut down. Total messages produced: %d", produced_count)


if __name__ == "__main__":
    main()

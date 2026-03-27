# Implementation Plan: Kafka Lakehouse POC -- Dual Architecture Comparison

## 1. CONTEXTUALIZATION

**Problem being solved.** This POC demonstrates and compares two common patterns for ingesting streaming data from Kafka into a lakehouse with medallion architecture (bronze/silver/gold):

- **Architecture 1 (Kafka Connect):** Uses Kafka Connect with a file/S3 sink connector to land raw files, then PySpark batch jobs to promote data through bronze and silver, and dbt for gold.
- **Architecture 2 (PySpark Streaming):** Uses PySpark Structured Streaming to consume directly from Kafka into bronze (skipping the landing/raw file stage), then PySpark for silver and dbt for gold.

**Scope boundaries.**

| In Scope | Out of Scope |
|---|---|
| Local Docker Compose deployment | Cloud deployment, Kubernetes |
| Single-node Kafka in KRaft mode | Multi-broker clusters |
| Synthetic e-commerce order data | Real data sources or CDC |
| Delta Lake as the table format | Iceberg, Hudi |
| dbt-spark for gold layer | dbt-trino, dbt-duckdb |
| Basic medallion transformations | ML pipelines, complex analytics |
| Basic observability (logs) | Prometheus/Grafana monitoring stack |

**Constraints.**
- Runs on a single developer machine (assume 16 GB RAM, 4-8 cores).
- No cloud services -- everything local via Docker Compose.
- No Zookeeper -- Kafka must use KRaft.

**Assumptions.**
- Delta Lake is acceptable as the open table format.
- A Spark Thrift Server will expose Delta tables to dbt-spark over JDBC/Thrift.
- The project uses a monorepo layout.

---

## 2. TECHNICAL DECISIONS

### Table Format: Delta Lake 3.2.x
- ACID transactions, schema enforcement, time travel, auto-compaction.

### Landing Layer Strategy (Architecture 1): Kafka Connect S3 Sink → MinIO
- `flush.size=10000` and `rotate.schedule.interval.ms=60000` to avoid small files.
- MinIO as local S3-compatible storage.

### Kafka: Single Broker, KRaft Mode
- `confluentinc/cp-kafka:7.6.0`, combined controller + broker.

### Spark: Bitnami Spark 3.5.x + Delta Lake 3.2.x
- Single master + single worker topology.

### dbt: dbt-spark with Thrift Server connection

### Schema Registry: Confluent Schema Registry with Avro serialization

### Dummy Data Domain: E-Commerce Orders (flat schema)

---

## 3. PROJECT FOLDER STRUCTURE

```
kafka_lakehouse/
├── PLAN.md
├── README.md
├── .env
├── .gitignore
├── shared/
│   ├── producer/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── producer.py
│   │   └── schemas/
│   │       └── order.avsc
│   ├── dbt_gold/
│   │   ├── dbt_project.yml
│   │   ├── profiles.yml
│   │   ├── Dockerfile
│   │   └── models/
│   │       ├── sources.yml
│   │       └── gold/
│   │           ├── daily_revenue.sql
│   │           ├── top_products.sql
│   │           └── order_status_summary.sql
│   └── spark/
│       ├── Dockerfile
│       └── spark-defaults.conf
├── arch1_kafka_connect/
│   ├── docker-compose.yml
│   ├── README.md
│   ├── connect/
│   │   └── config/
│   │       └── s3-sink.json
│   └── jobs/
│       ├── landing_to_bronze.py
│       └── bronze_to_silver.py
├── arch2_spark_streaming/
│   ├── docker-compose.yml
│   ├── README.md
│   └── jobs/
│       ├── kafka_to_bronze.py
│       └── bronze_to_silver.py
└── data/            # gitignored, Docker volume mounts
    ├── arch1/
    │   ├── landing/
    │   ├── bronze/
    │   ├── silver/
    │   └── gold/
    └── arch2/
        ├── bronze/
        ├── silver/
        └── gold/
```

---

## 4. DATA SCHEMA: ORDER EVENTS

**Avro schema (`order.avsc`):**
```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.poc.ecommerce",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "customer_id", "type": "string"},
    {"name": "customer_name", "type": "string"},
    {"name": "customer_email", "type": "string"},
    {"name": "order_status", "type": {"type": "enum", "name": "OrderStatus", "symbols": ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]}},
    {"name": "product_id", "type": "string"},
    {"name": "product_name", "type": "string"},
    {"name": "product_category", "type": "string"},
    {"name": "quantity", "type": "int"},
    {"name": "unit_price", "type": "double"},
    {"name": "total_amount", "type": "double"},
    {"name": "currency", "type": {"type": "enum", "name": "Currency", "symbols": ["USD", "EUR", "BRL"]}},
    {"name": "order_timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "shipping_address_city", "type": "string"},
    {"name": "shipping_address_state", "type": "string"},
    {"name": "shipping_address_country", "type": "string"}
  ]
}
```

---

## 5. LAYER TRANSFORMATIONS

| Layer | Input | Transformations | Output Format |
|---|---|---|---|
| **Landing** (arch 1 only) | Kafka topic | None (Kafka Connect writes as-is) | Parquet files, time-partitioned, batched |
| **Bronze** | Landing Parquet (arch 1) or Kafka stream (arch 2) | Add `_ingested_at`, `_source_file` (arch1) or `_kafka_offset`/`_kafka_partition` (arch2), `_batch_id` | Delta table, partitioned by `ingestion_date` |
| **Silver** | Bronze Delta | Cast types, validate constraints, deduplicate by `order_id` (latest by `order_timestamp`), derive `order_date` | Delta table, partitioned by `order_date` |
| **Gold** | Silver Delta (via dbt) | Aggregations: daily revenue, top products, order status counts | Delta tables |

---

## 6. DOCKER COMPOSE: ARCHITECTURE 1

| Service | Image | Memory Limit |
|---|---|---|
| `kafka` | `confluentinc/cp-kafka:7.6.0` | 1 GB |
| `schema-registry` | `confluentinc/cp-schema-registry:7.6.0` | 512 MB |
| `kafka-connect` | `confluentinc/cp-kafka-connect:7.6.0` | 1 GB |
| `minio` | `minio/minio:latest` | 512 MB |
| `producer` | Custom Python image | 256 MB |
| `spark-master` | Custom Spark 3.5 + Delta | 1 GB |
| `spark-worker` | Custom Spark 3.5 + Delta | 2 GB |
| `dbt` | Custom dbt-spark image | 512 MB |

Total: ~7.3 GB

## 7. DOCKER COMPOSE: ARCHITECTURE 2

Same as arch 1 minus `kafka-connect` and `minio`. Total: ~5.8 GB.

---

## 8. KEY CONFIGURATIONS

### Kafka KRaft (single combined node)
```
KAFKA_PROCESS_ROLES=broker,controller
KAFKA_NODE_ID=1
KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093
KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
```

### Kafka Connect S3 Sink (anti-small-file)
```json
{
  "connector.class": "io.confluent.connect.s3.S3SinkConnector",
  "tasks.max": "1",
  "topics": "orders.raw",
  "s3.bucket.name": "lakehouse",
  "store.url": "http://minio:9000",
  "flush.size": "10000",
  "rotate.schedule.interval.ms": "60000",
  "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
  "parquet.codec": "snappy",
  "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
  "partition.duration.ms": "3600000",
  "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH",
  "locale": "en-US",
  "timezone": "UTC",
  "timestamp.extractor": "Record",
  "value.converter": "io.confluent.connect.avro.AvroConverter",
  "value.converter.schema.registry.url": "http://schema-registry:8081"
}
```

### PySpark Structured Streaming (anti-small-file)
```python
spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "orders.raw") \
    .option("maxOffsetsPerTrigger", "10000") \
    .load() \
    .writeStream \
    .format("delta") \
    .option("checkpointLocation", "/data/arch2/bronze/_checkpoint") \
    .trigger(processingTime="60 seconds") \
    .start("/data/arch2/bronze/orders")
```

---

## 9. IMPLEMENTATION PHASES

### Phase 1: Foundation
1. Create project skeleton (directories, `.gitignore`, `.env`)
2. Write Avro schema (`order.avsc`)
3. Build infinite dummy producer (`producer.py` using Faker + confluent-kafka)
4. Build shared custom Spark Docker image (Bitnami + Delta JARs + Kafka JARs)
5. Validate Kafka KRaft startup

### Phase 2: Architecture 1 (Kafka Connect)
1. Write `arch1_kafka_connect/docker-compose.yml`
2. Configure MinIO bucket creation and S3 Sink connector auto-submit
3. Implement `landing_to_bronze.py` (PySpark batch: MinIO Parquet → Bronze Delta)
4. Implement `bronze_to_silver.py` (PySpark batch: Bronze → Silver Delta with dedup/cast)
5. Implement dbt gold models and `profiles.yml`
6. End-to-end validation

### Phase 3: Architecture 2 (PySpark Streaming)
1. Write `arch2_spark_streaming/docker-compose.yml`
2. Implement `kafka_to_bronze.py` (PySpark Structured Streaming → Bronze Delta)
3. Implement `bronze_to_silver.py` (same logic as arch 1, different paths)
4. Run shared dbt gold models against arch 2 silver
5. End-to-end validation

### Phase 4: Documentation
1. Write root `README.md` with setup instructions and architecture diagrams
2. Write per-architecture READMEs
3. Write architecture comparison table

---

## 10. TECHNOLOGY VERSIONS

| Component | Version | Image |
|---|---|---|
| Kafka (KRaft) | 7.6.0 (Confluent) | `confluentinc/cp-kafka:7.6.0` |
| Schema Registry | 7.6.0 | `confluentinc/cp-schema-registry:7.6.0` |
| Kafka Connect | 7.6.0 | `confluentinc/cp-kafka-connect:7.6.0` |
| S3 Sink Connector | 10.5.x | Confluent Hub install |
| MinIO | latest | `minio/minio:latest` |
| Spark | 3.5.1 | `bitnami/spark:3.5` (custom) |
| Delta Lake | 3.2.0 | JAR added to Spark image |
| Python (producer) | 3.11 | `python:3.11-slim` |
| dbt-core | 1.8.x | pip install |
| dbt-spark | 1.8.x | pip install |

---

## 11. RISKS

1. **Avro deserialization in PySpark Streaming:** Confluent wire format has a 5-byte header not handled by `from_avro()` natively. Use ABRiS library or strip header manually. Fallback: JSON serialization.
2. **Spark Thrift Server stability:** Can be flaky under memory pressure. Use Docker `restart: on-failure`.
3. **Docker resource contention:** Total ~7.3 GB for arch 1. Close other applications.
4. **Delta/Spark version mismatch:** Delta 3.2.x requires Spark 3.5.x exactly. Pin all versions in `.env`.

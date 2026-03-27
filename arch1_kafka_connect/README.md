# Architecture 1: Kafka Connect + Batch PySpark

## Overview

This architecture uses **Kafka Connect** with the Confluent S3 Sink connector to
continuously land raw Parquet files into MinIO (local S3-compatible storage).
PySpark batch jobs then promote data through the Bronze and Silver Delta layers
on a schedule. dbt aggregates the Silver layer into Gold Delta tables via the
Spark Thrift Server.

```
Producer → Kafka → Kafka Connect → MinIO (Parquet)
                                        ↓
                               landing_to_bronze.py (PySpark batch)
                                        ↓
                                  Bronze Delta Table
                                        ↓
                               bronze_to_silver.py (PySpark batch)
                                        ↓
                                  Silver Delta Table
                                        ↓
                                    dbt run
                                        ↓
                                  Gold Delta Tables
```

## Services

| Container | Purpose | Ports |
|---|---|---|
| `arch1-kafka` | Kafka broker (KRaft mode) | 9092 |
| `arch1-schema-registry` | Avro Schema Registry | 8081 |
| `arch1-minio` | S3-compatible object storage | 9000 (API), 9001 (Console) |
| `arch1-minio-init` | Creates `lakehouse` bucket, then exits | - |
| `arch1-kafka-connect` | Installs and runs S3 Sink connector | 8083 |
| `arch1-connector-init` | POSTs connector config, then exits | - |
| `arch1-producer` | Infinite Avro event producer | - |
| `arch1-spark-master` | Spark master + Thrift Server | 8080, 7077, 10000 |
| `arch1-spark-worker` | Spark executor | 8081 |
| `arch1-dbt` | dbt gold models (profile: dbt) | - |

## Prerequisites

- Docker 24+ and Docker Compose v2
- ~7.5 GB free RAM
- ~5 GB free disk

## Quick Start

```bash
cd arch1_kafka_connect

# Build all images and start services
docker compose up -d --build

# Follow all logs
docker compose logs -f

# Watch only producer
docker compose logs -f producer
```

## Startup Sequence

The startup has a deliberate dependency chain:

1. `kafka` starts and becomes healthy (~30 s)
2. `schema-registry` starts (depends on kafka health)
3. `minio` starts in parallel
4. `minio-init` creates the `lakehouse` bucket (depends on minio health)
5. `kafka-connect` installs the S3 Sink connector plugin and starts (~90 s total)
6. `connector-init` POSTs the connector configuration (depends on connect health)
7. `producer` starts publishing events (depends on kafka + schema-registry health)
8. `spark-master` starts and launches the Thrift Server in the background
9. `spark-worker` registers with the master

## Verifying Each Layer

### 1. Producer is publishing events

```bash
# Check producer logs for "Produced N messages"
docker compose logs producer | tail -20

# List registered schemas
curl -s http://localhost:8081/subjects
# Expected: ["orders.raw-value"]
```

### 2. Kafka Connect S3 Sink is running

```bash
# Check connector status
curl -s http://localhost:8083/connectors/s3-sink-orders/status | python3 -m json.tool

# Expected output contains:
# "connector": { "state": "RUNNING" }
# "tasks": [{ "state": "RUNNING" }]
```

### 3. Parquet files appear in MinIO

Open the MinIO console at http://localhost:9001 (minioadmin / minioadmin).
Navigate to `lakehouse` → `topics` → `orders.raw` → you should see
time-partitioned directories after the connector accumulates 10 000 messages
or 60 seconds (whichever comes first).

```bash
# Alternatively, use the mc CLI
docker compose exec minio-init mc ls myminio/lakehouse/topics/orders.raw --recursive
```

### 4. Landing → Bronze

```bash
docker compose exec spark-master \
    spark-submit \
    --master spark://spark-master:7077 \
    --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" \
    --conf "spark.hadoop.fs.s3a.access.key=minioadmin" \
    --conf "spark.hadoop.fs.s3a.secret.key=minioadmin" \
    --conf "spark.hadoop.fs.s3a.path.style.access=true" \
    /jobs/arch1/landing_to_bronze.py

# Verify Delta table exists
docker compose exec spark-master \
    /opt/bitnami/spark/bin/spark-sql \
    --master spark://spark-master:7077 \
    -e "SELECT COUNT(*), MIN(ingestion_date), MAX(ingestion_date) FROM delta.\`/data/arch1/bronze/orders\`;"
```

### 5. Bronze → Silver

```bash
docker compose exec spark-master \
    spark-submit \
    --master spark://spark-master:7077 \
    /jobs/arch1/bronze_to_silver.py

# Verify Silver table
docker compose exec spark-master \
    /opt/bitnami/spark/bin/spark-sql \
    --master spark://spark-master:7077 \
    -e "SELECT COUNT(*), MIN(order_date), MAX(order_date) FROM delta.\`/data/arch1/silver/orders\`;"
```

### 6. Gold (dbt)

```bash
# dbt is defined under the 'dbt' profile so it doesn't start automatically
docker compose --profile dbt run --rm dbt

# Check gold tables via Thrift Server
docker compose exec spark-master \
    /opt/bitnami/spark/bin/beeline \
    -u "jdbc:hive2://localhost:10000" \
    -e "SELECT * FROM gold.daily_revenue LIMIT 5;"
```

## Connector Configuration Details

The S3 Sink connector is configured in `connect/config/s3-sink.json`:

| Setting | Value | Rationale |
|---|---|---|
| `flush.size` | 10000 | Write a file after every 10k messages |
| `rotate.schedule.interval.ms` | 60000 | Force a file rotation every 60 seconds even if flush.size not reached |
| `format.class` | ParquetFormat | Columnar format efficient for Spark reads |
| `parquet.codec` | snappy | Good balance of compression speed vs. ratio |
| `partitioner.class` | TimeBasedPartitioner | Year/month/day/hour partitioning |
| `timestamp.extractor` | Record | Uses Kafka message timestamp for partitioning |

## Troubleshooting

**Connector stays in FAILED state:**
```bash
# Check Connect worker logs
docker compose logs kafka-connect | grep ERROR

# Delete and re-submit the connector
curl -X DELETE http://localhost:8083/connectors/s3-sink-orders
curl -X POST http://localhost:8083/connectors \
     -H 'Content-Type: application/json' \
     -d @connect/config/s3-sink.json
```

**Thrift Server not responding on port 10000:**
```bash
# Check spark-master logs
docker compose logs spark-master | grep -i thrift

# The entrypoint script retries; give it 60-90 seconds to start
```

**MinIO bucket not found:**
```bash
# Re-run the init container
docker compose run --rm minio-init
```

**Out of memory errors in Spark:**
- Reduce `SPARK_WORKER_MEMORY` in docker-compose.yml
- Close other applications on your machine
- Increase Docker Desktop memory limit (if on macOS/Windows)

## Tear Down

```bash
docker compose down -v    # Remove containers and volumes (including MinIO data)
```

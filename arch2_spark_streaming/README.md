# Architecture 2: PySpark Structured Streaming

## Overview

This architecture uses **PySpark Structured Streaming** to consume Avro-encoded
messages directly from Kafka and write them to a Bronze Delta table as
micro-batches. There is no landing/raw file layer and no dependency on Kafka
Connect or MinIO.

PySpark batch jobs then promote data through the Silver layer, and dbt
aggregates Silver into Gold Delta tables via the Spark Thrift Server.

```
Producer → Kafka → kafka_to_bronze.py (Structured Streaming)
                             ↓
                      Bronze Delta Table (micro-batch, 60 s trigger)
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
| `arch2-kafka` | Kafka broker (KRaft mode) | 9092 |
| `arch2-schema-registry` | Avro Schema Registry | 8081 |
| `arch2-producer` | Infinite Avro event producer | - |
| `arch2-spark-master` | Spark master + Thrift Server | 8080, 7077, 10000 |
| `arch2-spark-worker` | Spark executor | 8082 |
| `arch2-dbt` | dbt gold models (profile: dbt) | - |

## Prerequisites

- Docker 24+ and Docker Compose v2
- ~6 GB free RAM
- ~3 GB free disk

## Quick Start

```bash
cd arch2_spark_streaming

# Build all images and start services
docker compose up -d --build

# Follow all logs
docker compose logs -f
```

## Running the Streaming Bronze Job

The streaming job is not started automatically by Docker Compose -- it must be
submitted as a `spark-submit` job. This allows you to control when consumption
begins and to restart it independently.

```bash
# Submit the streaming job (runs indefinitely)
docker compose exec spark-master \
    spark-submit \
    --master spark://spark-master:7077 \
    /jobs/arch2/kafka_to_bronze.py
```

The job runs until the container or process is terminated. To run it in the
background:

```bash
# In a separate terminal, stream logs from spark-master
docker compose logs -f spark-master
```

### Streaming Configuration

| Parameter | Value | Effect |
|---|---|---|
| `maxOffsetsPerTrigger` | 10000 | Limit per micro-batch to avoid OOM |
| `trigger(processingTime)` | 60 seconds | Micro-batch frequency (balances latency vs. file count) |
| `startingOffsets` | earliest | Process all available messages on first run |
| `failOnDataLoss` | false | Continue if Kafka topic data was deleted |
| `checkpointLocation` | `/data/arch2/bronze/_checkpoint` | Fault-tolerant offset tracking |

## Verifying Each Layer

### 1. Producer is publishing events

```bash
docker compose logs producer | tail -20
curl -s http://localhost:8081/subjects
# Expected: ["orders.raw-value"]
```

### 2. Bronze Delta table is being populated

```bash
# After the streaming job's first trigger fires (~60 s):
docker compose exec spark-master \
    /opt/bitnami/spark/bin/spark-sql \
    --master spark://spark-master:7077 \
    -e "SELECT COUNT(*), MIN(_ingested_at), MAX(_ingested_at) FROM delta.\`/data/arch2/bronze/orders\`;"
```

You should see the row count increase each time you run this command.

### 3. Avro deserialization is working

The streaming job strips the Confluent 5-byte wire-format header before calling
`from_avro()`. Verify the schema loaded correctly:

```bash
docker compose logs spark-master | grep "Loaded Avro schema"
```

### 4. Bronze → Silver

```bash
docker compose exec spark-master \
    spark-submit \
    --master spark://spark-master:7077 \
    /jobs/arch2/bronze_to_silver.py

# Verify Silver
docker compose exec spark-master \
    /opt/bitnami/spark/bin/spark-sql \
    --master spark://spark-master:7077 \
    -e "SELECT COUNT(*), COUNT(DISTINCT order_id) FROM delta.\`/data/arch2/silver/orders\`;"
```

### 5. Gold (dbt)

```bash
docker compose --profile dbt run --rm dbt

docker compose exec spark-master \
    /opt/bitnami/spark/bin/beeline \
    -u "jdbc:hive2://localhost:10000" \
    -e "SELECT * FROM gold.daily_revenue ORDER BY order_date DESC LIMIT 10;"
```

## Avro Header Stripping: Technical Detail

The Confluent Schema Registry wire format prefixes every Avro-serialized message
with a 5-byte envelope:

```
Byte 0:    0x00 (magic byte)
Bytes 1-4: Schema ID (big-endian int32)
Bytes 5+:  Binary Avro payload
```

PySpark's built-in `from_avro()` function (from `pyspark.sql.avro.functions`)
expects a raw Avro binary payload with no prefix. The `kafka_to_bronze.py` job
uses a Python UDF to strip the first 5 bytes:

```python
def _strip(payload: bytes) -> bytes:
    if payload is None or len(payload) < 5:
        return payload
    if payload[0] != 0x00:
        return payload  # Not Confluent format; pass through
    return bytes(payload[5:])
```

After stripping, the cleaned bytes are passed to `from_avro()` with the schema
string loaded from `schemas/order.avsc`. This approach avoids the ABRiS library
dependency (which has complex version requirements).

## Delta Table Delta Log

You can inspect the Bronze transaction log to see each micro-batch as a
committed transaction:

```bash
ls /home/victor-henrique/Documents/repos/work/pocs/kafka_lakehouse/data/arch2/bronze/orders/_delta_log/
```

Each `.json` file in `_delta_log/` corresponds to one committed micro-batch
from the streaming query.

## Checkpoint Recovery

If the streaming job crashes and is restarted, it will resume from the last
committed Kafka offset stored in `/data/arch2/bronze/_checkpoint/`. No
messages will be replayed or skipped.

To restart from the beginning of the Kafka topic (full replay):

```bash
# Delete the checkpoint
rm -rf /home/victor-henrique/Documents/repos/work/pocs/kafka_lakehouse/data/arch2/bronze/_checkpoint

# Restart the job -- it will start from earliest offsets
docker compose exec spark-master \
    spark-submit \
    --master spark://spark-master:7077 \
    /jobs/arch2/kafka_to_bronze.py
```

Note: Deleting the checkpoint causes the Silver MERGE to handle duplicates
correctly (the merge key is `order_id`).

## Troubleshooting

**`from_avro` parse errors in Spark logs:**
```bash
docker compose logs spark-master | grep "from_avro\|AvroDeserializer\|FAILED"
```
- Verify the schema file is mounted at `/schemas/order.avsc` inside the container.
- Check that the producer is using the same schema version.

**No data in Bronze after 2+ minutes:**
```bash
# Check streaming query status
docker compose logs spark-master | grep "Batch\|trigger\|Processing"
```

**Spark Thrift Server port 10000 not listening:**
```bash
docker compose exec spark-master netstat -tlnp | grep 10000
# If not listening, check entrypoint logs:
docker compose logs spark-master | grep -i thrift
```

## Tear Down

```bash
docker compose down -v
```

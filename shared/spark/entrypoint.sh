#!/bin/bash
# Spark entrypoint: delegates to original Bitnami entrypoint, then optionally
# starts the Thrift Server when this node is the master.
#
# Environment variables:
#   SPARK_MODE          - "master" | "worker" (set by Bitnami image)
#   START_THRIFT_SERVER - "true" to enable Thrift Server on master
#   ARCH_DATA_PATH      - base path for lakehouse data, e.g. /data/arch1

set -euo pipefail

BITNAMI_ENTRYPOINT="/opt/bitnami/scripts/spark/entrypoint.sh"
BITNAMI_RUN="/opt/bitnami/scripts/spark/run.sh"

# ---------------------------------------------------------------------------
# Start the Thrift Server (master-only, when requested)
# ---------------------------------------------------------------------------
start_thrift_server() {
    local arch_data_path="${ARCH_DATA_PATH:-/data/arch1}"
    local silver_path="${arch_data_path}/silver/orders"
    local gold_path="${arch_data_path}/gold"

    echo "[entrypoint] Waiting for Spark master REST API to be available..."
    local retries=0
    # Use /dev/tcp (bash built-in) to check port 8080 -- no curl/nc needed
    until (echo > /dev/tcp/localhost/8080) 2>/dev/null; do
        retries=$((retries + 1))
        if [ "$retries" -ge 30 ]; then
            echo "[entrypoint] Spark master did not become ready in time."
            break
        fi
        sleep 2
    done

    echo "[entrypoint] Starting Spark Thrift Server on port 10000..."
    /opt/bitnami/spark/sbin/start-thriftserver.sh \
        --master "spark://$(hostname):7077" \
        --hiveconf hive.server2.thrift.port=10000 \
        --hiveconf hive.server2.thrift.bind.host=0.0.0.0 \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        --conf spark.driver.memory=512m \
        --conf spark.executor.memory=1g \
        --conf spark.sql.shuffle.partitions=4 \
        2>&1 &

    echo "[entrypoint] Waiting for Thrift Server to start on port 10000..."
    local ts_retries=0
    # Use /dev/tcp bash built-in to probe port 10000
    until (echo > /dev/tcp/localhost/10000) 2>/dev/null; do
        ts_retries=$((ts_retries + 1))
        if [ "$ts_retries" -ge 60 ]; then
            echo "[entrypoint] WARNING: Thrift Server did not become ready in time."
            break
        fi
        sleep 3
    done

    echo "[entrypoint] Registering Delta tables in Spark catalog..."
    /opt/bitnami/spark/bin/spark-sql \
        --master "spark://$(hostname):7077" \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        -e "
            CREATE DATABASE IF NOT EXISTS silver;
            CREATE TABLE IF NOT EXISTS silver.orders
                USING DELTA
                LOCATION '${silver_path}';
            CREATE DATABASE IF NOT EXISTS gold;
        " 2>&1 || echo "[entrypoint] WARNING: Table registration failed (tables may not exist yet)."

    echo "[entrypoint] Thrift Server setup complete."
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
# Run the Bitnami entrypoint in the background so we can hook in after start
"$BITNAMI_ENTRYPOINT" "$BITNAMI_RUN" &
BITNAMI_PID=$!

if [ "${SPARK_MODE:-}" = "master" ] && [ "${START_THRIFT_SERVER:-false}" = "true" ]; then
    start_thrift_server
fi

# Wait for the Bitnami process to exit (keeps container alive)
wait "$BITNAMI_PID"

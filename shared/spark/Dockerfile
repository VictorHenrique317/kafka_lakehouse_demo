FROM bitnamilegacy/spark:3.5.1

USER root

# ---------------------------------------------------------------------------
# JAR dependencies
# ---------------------------------------------------------------------------
# All JARs are fetched from Maven Central and placed in the Spark jars dir.
# Versions are pinned to match the Spark 3.5.1 + Delta 3.2.0 matrix.

ENV SPARK_JARS_DIR=/opt/bitnami/spark/jars
ENV MVN=https://repo1.maven.org/maven2

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

RUN curl -fSL "${MVN}/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar" \
         -o "${SPARK_JARS_DIR}/delta-spark_2.12-3.2.0.jar" && \
    curl -fSL "${MVN}/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar" \
         -o "${SPARK_JARS_DIR}/delta-storage-3.2.0.jar" && \
    curl -fSL "${MVN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar" \
         -o "${SPARK_JARS_DIR}/spark-sql-kafka-0-10_2.12-3.5.1.jar" && \
    curl -fSL "${MVN}/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar" \
         -o "${SPARK_JARS_DIR}/kafka-clients-3.5.1.jar" && \
    curl -fSL "${MVN}/org/apache/spark/spark-avro_2.12/3.5.1/spark-avro_2.12-3.5.1.jar" \
         -o "${SPARK_JARS_DIR}/spark-avro_2.12-3.5.1.jar" && \
    curl -fSL "${MVN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar" \
         -o "${SPARK_JARS_DIR}/spark-token-provider-kafka-0-10_2.12-3.5.1.jar" && \
    curl -fSL "${MVN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" \
         -o "${SPARK_JARS_DIR}/commons-pool2-2.11.1.jar" && \
    curl -fSL "${MVN}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
         -o "${SPARK_JARS_DIR}/hadoop-aws-3.3.4.jar" && \
    curl -fSL "${MVN}/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" \
         -o "${SPARK_JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar"

COPY spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

# Entrypoint script to optionally start Thrift Server on the master node
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER 1001

ENTRYPOINT ["/entrypoint.sh"]

FROM bitnami/spark:3.5.6

# Start as root to perform administrative tasks
USER root

# 1. Create the non-root user
RUN adduser --disabled-password --gecos '' --uid 1000 sparkuser

# 2. Grant ownership of the Spark directory
RUN chown -R sparkuser:sparkuser /opt/bitnami/spark

# 3. Create and grant ownership of the checkpoint directory
RUN mkdir -p /tmp/spark_checkpoints && chown -R sparkuser:sparkuser /tmp/spark_checkpoints

# 4. Install Kafka connector JARs for Spark 3.5.6
RUN curl -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.6.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.6/spark-sql-kafka-0-10_2.12-3.5.6.jar && \
    curl -o /opt/bitnami/spark/jars/kafka-clients-3.6.0.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.0/kafka-clients-3.6.0.jar && \
    curl -o /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.6.jar https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.6/spark-token-provider-kafka-0-10_2.12-3.5.6.jar && \
    curl -o /opt/bitnami/spark/jars/commons-pool2-2.11.1.jar https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# 5. Install Kafka client for debugging
RUN apt-get update && apt-get install -y wget && \
    wget https://archive.apache.org/dist/kafka/3.6.0/kafka_2.12-3.6.0.tgz -O /tmp/kafka.tgz && \
    tar -xzf /tmp/kafka.tgz -C /opt/ && \
    mv /opt/kafka_2.12-3.6.0 /opt/kafka && \
    rm /tmp/kafka.tgz && \
    apt-get remove -y wget && apt-get autoremove -y && apt-get clean

# 6. Switch to the non-root user for runtime
USER sparkuser
FROM bitnami/spark:3.5

# Start as root to perform administrative tasks
USER root

# 1. Create the non-root user
RUN adduser --disabled-password --gecos '' --uid 1000 sparkuser

# 2. Grant ownership of the Spark directory
RUN chown -R sparkuser:sparkuser /opt/bitnami/spark

# 3. <<< NEW STEP >>> Create and grant ownership of the checkpoint directory
# This ensures the volume mount point is writable by sparkuser.
RUN mkdir -p /tmp/spark_checkpoints && chown -R sparkuser:sparkuser /tmp/spark_checkpoints

# 4. Bake the necessary JAR files into the image
RUN curl -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -o /opt/bitnami/spark/jars/kafka-clients-3.5.0.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar && \
    curl -o /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -o /opt/bitnami/spark/jars/commons-pool2-2.11.1.jar https://repo1.maven.org/maven2/org/apache/commons-pool/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# 5. Switch to the new non-root user for runtime
USER sparkuser
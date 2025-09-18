FROM bitnami/spark:3.5

# Start as root to perform administrative tasks
USER root

# 1. Create the non-root user
RUN adduser --disabled-password --gecos '' --uid 1000 sparkuser

# 2. Grant the new user ownership of the Spark directory
# This command recursively (-R) changes the owner and group of the entire directory.
RUN chown -R sparkuser:sparkuser /opt/bitnami/spark

# 3. Bake the necessary JAR files into the image
RUN curl -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -o /opt/bitnami/spark/jars/kafka-clients-3.5.0.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar && \
    curl -o /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -o /opt/bitnami/spark/jars/commons-pool2-2.11.1.jar https://repo1.maven.org/maven2/org/apache/commons-pool/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# 4. Switch to the new non-root user for runtime
USER sparkuser
# Spark 3.5 + the connectors the streaming job needs, baked in at build time
# (reproducible, and no downloads when the container starts).
FROM apache/spark:3.5.3-python3

USER root
ARG MAVEN=https://repo1.maven.org/maven2

# Kafka source for Structured Streaming
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/
# S3A filesystem (must match Spark's bundled Hadoop 3.3.4)
ADD --chmod=644 ${MAVEN}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/

RUN pip install --no-cache-dir pytest==8.3.4

WORKDIR /opt/app
COPY jobs ./jobs
COPY tests ./tests
COPY conftest.py ./
RUN mkdir -p /checkpoints && chown spark /checkpoints

USER spark
# pyspark ships inside /opt/spark/python; expose it so plain `python3` / pytest can import it
ENV PYTHONPATH=/opt/app:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[2]", \
     "--driver-memory", "768m", \
     "/opt/app/jobs/silver_vehicle_positions.py"]

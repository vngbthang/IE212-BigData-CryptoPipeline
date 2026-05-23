FROM apache/spark:3.5.8-scala2.12-java17-python3-r-ubuntu

USER root

RUN apt-get update && \
    apt-get install -y wget && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /tmp/spark-jars && \
    wget -q -O /tmp/spark-jars/iceberg-spark-runtime.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.8.0/iceberg-spark-runtime-3.5_2.12-1.8.0.jar && \
    wget -q -O /tmp/spark-jars/nessie-spark-extensions.jar \
      https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.102.5/nessie-spark-extensions-3.5_2.12-0.102.5.jar && \
    wget -q -O /tmp/spark-jars/hadoop-aws.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -q -O /tmp/spark-jars/aws-java-sdk-bundle.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    wget -q -O /tmp/spark-jars/spark-sql-kafka.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    wget -q -O /tmp/spark-jars/spark-token-provider.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    wget -q -O /tmp/spark-jars/kafka-clients.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    wget -q -O /tmp/spark-jars/commons-pool2.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    cp /tmp/spark-jars/*.jar /opt/spark/jars/ && \
    rm -rf /tmp/spark-jars

USER 0

FROM apache/airflow:2.9.3-python3.8

USER root

ENV SPARK_VERSION=3.5.6
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${SPARK_HOME}/bin:${PATH}"

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
    && curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" -o /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
    && rm /tmp/spark.tgz \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
ARG SPARK_VERSION=3.5.3
ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.367
ARG POSTGRES_JDBC_VERSION=42.7.3
ARG HIVE_VERSION=3.1.3

FROM apache/spark:${SPARK_VERSION} AS spark-dist

FROM apache/airflow:2.7.0

ARG SPARK_VERSION
ARG HADOOP_VERSION
ARG AWS_SDK_VERSION
ARG POSTGRES_JDBC_VERSION
ARG HIVE_VERSION

# Install Airflow providers first to leverage Docker layer cache
RUN --mount=type=cache,target=/home/airflow/.cache/pip \
    pip install apache-airflow-providers-apache-spark==4.1.3 && \
    pip install neo4j

USER root

# Install Java and utilities in a single layer for better caching
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jdk procps curl && \
    rm -rf /var/lib/apt/lists/*

# Normalize JAVA_HOME across architectures and ensure java is on PATH
RUN arch=$(dpkg --print-architecture) && \
    if [ "$arch" = "amd64" ]; then \
      ln -s /usr/lib/jvm/java-11-openjdk-amd64 /usr/lib/jvm/java-11-openjdk; \
    else \
      ln -s /usr/lib/jvm/java-11-openjdk-arm64 /usr/lib/jvm/java-11-openjdk; \
    fi
ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Set environment variables
ENV SPARK_HOME="/opt/spark"
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

# Reuse Spark installation from official image to avoid re-downloading tarballs
COPY --from=spark-dist /opt/spark /opt/spark

# Add extra JARs once; versions controlled by ARGs for cache-friendly bumps
RUN curl -fsSL -o ${SPARK_HOME}/jars/hadoop-aws-${HADOOP_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar && \
    curl -fsSL -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar && \
    curl -fsSL -o ${SPARK_HOME}/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar \
      https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar && \
    curl -fsSL -o ${SPARK_HOME}/jars/hive-metastore-${HIVE_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/hive/hive-metastore/${HIVE_VERSION}/hive-metastore-${HIVE_VERSION}.jar && \
    curl -fsSL -o ${SPARK_HOME}/jars/hive-exec-${HIVE_VERSION}-core.jar \
      https://repo1.maven.org/maven2/org/apache/hive/hive-exec/${HIVE_VERSION}/hive-exec-${HIVE_VERSION}-core.jar && \
    curl -fsSL -o ${SPARK_HOME}/jars/hive-common-${HIVE_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/hive/hive-common/${HIVE_VERSION}/hive-common-${HIVE_VERSION}.jar && \
    curl -fsSL -o ${SPARK_HOME}/jars/hive-serde-${HIVE_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/hive/hive-serde/${HIVE_VERSION}/hive-serde-${HIVE_VERSION}.jar && \
    curl -fsSL -o ${SPARK_HOME}/jars/hive-standalone-metastore-${HIVE_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/hive/hive-standalone-metastore/${HIVE_VERSION}/hive-standalone-metastore-${HIVE_VERSION}.jar && \
    curl -fsSL -o ${SPARK_HOME}/jars/libthrift-0.12.0.jar \
      https://repo1.maven.org/maven2/org/apache/thrift/libthrift/0.12.0/libthrift-0.12.0.jar && \
    chown -R airflow:root ${SPARK_HOME}

# Copy Spark configuration late to avoid invalidating heavy layers
COPY ./spark-defaults.conf "${SPARK_HOME}/conf/spark-defaults.conf"

USER airflow

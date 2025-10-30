ARG SPARK_VERSION=3.5.3
ARG HADOOP_VERSION=2.10.2
ARG AWS_SDK_VERSION=1.12.367
ARG POSTGRES_JDBC_VERSION=42.7.3
ARG HIVE_VERSION=2.3.9

FROM spark-with-jars:${SPARK_VERSION} AS spark-dist

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

# Fix permissions (JARs are already copied from spark-with-jars image)
RUN chown -R airflow:root ${SPARK_HOME}

# Copy Spark configuration late to avoid invalidating heavy layers
COPY ./spark-defaults.conf "${SPARK_HOME}/conf/spark-defaults.conf"

USER airflow

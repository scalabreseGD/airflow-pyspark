# syntax=docker/dockerfile:1.6

ARG SPARK_VERSION=3.5.3
ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.367
ARG POSTGRES_JDBC_VERSION=42.7.3
ARG HIVE_VERSION=3.1.3

FROM apache/spark:${SPARK_VERSION}

ARG HADOOP_VERSION
ARG AWS_SDK_VERSION
ARG POSTGRES_JDBC_VERSION
ARG HIVE_VERSION

USER root

# Use distro Python and tools; avoid building Python from source
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip curl && \
    rm -rf /var/lib/apt/lists/*

# Install Python packages with cache mount
RUN --mount=type=cache,target=/root/.cache/pip \
    python3 -m pip install --no-cache-dir --upgrade pip && \
    python3 -m pip install --no-cache-dir neo4j

# Ensure Spark uses system python3
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3

# Remove Spark's built-in Hive JARs to avoid version conflicts
RUN cd /opt/spark/jars && \
    rm -f hive-* datanucleus-* javax.jdo-*.jar

# Add required JARs for S3/Hive integrations (versions provided via build args)
RUN cd /opt/spark/jars && \
    # S3/AWS dependencies
    curl -fsSL -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar && \
    # Hive 3.1.3 metastore dependencies
    curl -fsSL -O https://repo1.maven.org/maven2/org/apache/hive/hive-metastore/${HIVE_VERSION}/hive-metastore-${HIVE_VERSION}.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/org/apache/hive/hive-exec/${HIVE_VERSION}/hive-exec-${HIVE_VERSION}-core.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/org/apache/hive/hive-common/${HIVE_VERSION}/hive-common-${HIVE_VERSION}.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/org/apache/hive/hive-serde/${HIVE_VERSION}/hive-serde-${HIVE_VERSION}.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/org/apache/hive/hive-standalone-metastore/${HIVE_VERSION}/hive-standalone-metastore-${HIVE_VERSION}.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/org/apache/thrift/libthrift/0.12.0/libthrift-0.12.0.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/org/apache/hive/hive-storage-api/2.7.3/hive-storage-api-2.7.3.jar && \
    # Apache Calcite (required by Hive 3.x)
    curl -fsSL -O https://repo1.maven.org/maven2/org/apache/calcite/calcite-core/1.16.0/calcite-core-1.16.0.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/org/apache/calcite/avatica/avatica-core/1.16.0/avatica-core-1.16.0.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/org/apache/calcite/calcite-druid/1.16.0/calcite-druid-1.16.0.jar && \
    # DataNucleus + JDO (required by Hive metastore)
    curl -fsSL -O https://repo1.maven.org/maven2/org/datanucleus/datanucleus-api-jdo/5.2.6/datanucleus-api-jdo-5.2.6.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/org/datanucleus/datanucleus-core/5.2.8/datanucleus-core-5.2.8.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/org/datanucleus/datanucleus-rdbms/5.2.11/datanucleus-rdbms-5.2.11.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/org/datanucleus/javax.jdo/3.2.0-m13/javax.jdo-3.2.0-m13.jar && \
    # Guava (Hive 3.x compatibility)
    curl -fsSL -O https://repo1.maven.org/maven2/com/google/guava/guava/27.0-jre/guava-27.0-jre.jar

USER spark
WORKDIR /opt/spark

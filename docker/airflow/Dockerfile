FROM apache/airflow:2.7.0

# Install Spark provider first as airflow user
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==4.1.3
RUN pip install --no-cache-dir neo4j

USER root

# Install Java 11 and utilities
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Dynamically detect Java path for ARM64/AMD64 compatibility
RUN DETECTED_JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java)))) && \
    echo "Detected JAVA_HOME: ${DETECTED_JAVA_HOME}"

# Set environment variables - will work for both amd64 and arm64
ENV SPARK_HOME="/opt/spark"
ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-arm64"
ENV PATH="${JAVA_HOME}/bin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

# Set Spark cluster connection details
ENV SPARK_MASTER_PORT="7077"
ENV SPARK_MASTER_HOST="spark-master"

# Configure Spark download sources (CDN + fallbacks)
ARG SPARK_VERSION=3.5.3
ARG SPARK_MIRROR_PRIMARY=https://dlcdn.apache.org/spark
ARG SPARK_MIRROR_FALLBACK=https://downloads.apache.org/spark
ARG SPARK_ARCHIVE=https://archive.apache.org/dist/spark

# Download and install Spark with mirror/CDN fallbacks (no closer.lua to avoid HTML)
RUN mkdir -p ${SPARK_HOME} && \
    ( \
      curl -fSL --retry 5 --retry-delay 2 -o /tmp/spark.tgz ${SPARK_MIRROR_PRIMARY}/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
      || curl -fSL --retry 5 --retry-delay 2 -o /tmp/spark.tgz ${SPARK_MIRROR_FALLBACK}/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
      || curl -fSL --retry 5 --retry-delay 2 -o /tmp/spark.tgz ${SPARK_ARCHIVE}/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    ) && \
    tar tzf /tmp/spark.tgz >/dev/null && \
    tar xzf /tmp/spark.tgz --directory ${SPARK_HOME} --strip-components 1 && \
    rm -f /tmp/spark.tgz

# Download additional JARs for S3/MinIO support via CDN-backed Maven
ARG MAVEN_BASE=https://repo.maven.apache.org/maven2
RUN curl -fSL -o ${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar \
      ${MAVEN_BASE}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fSL -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar \
      ${MAVEN_BASE}/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Copy Spark configuration
COPY ./spark-defaults.conf "${SPARK_HOME}/conf/spark-defaults.conf"

# Set ownership to airflow user
RUN chown -R airflow:root ${SPARK_HOME}

# Install procps for process monitoring
RUN apt-get update && \
    apt-get install -y procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

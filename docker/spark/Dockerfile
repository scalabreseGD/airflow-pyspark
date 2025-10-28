FROM apache/spark:3.5.3

USER root

# Install build dependencies and Python 3.11 from source
RUN apt-get update && \
    apt-get install -y wget curl build-essential libssl-dev zlib1g-dev \
    libbz2-dev libreadline-dev libsqlite3-dev libncursesw5-dev \
    xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev && \
    cd /tmp && \
    wget https://www.python.org/ftp/python/3.11.6/Python-3.11.6.tgz && \
    tar xzf Python-3.11.6.tgz && \
    cd Python-3.11.6 && \
    ./configure --enable-optimizations --with-ensurepip=install && \
    make -j$(nproc) && \
    make altinstall && \
    cd /tmp && \
    rm -rf Python-3.11.6* && \
    apt-get remove -y build-essential && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

# Set Python 3.11 as the default python3
RUN update-alternatives --install /usr/bin/python3 python3 /usr/local/bin/python3.11 1 && \
    update-alternatives --set python3 /usr/local/bin/python3.11

# Install Python packages needed by jobs
RUN python3 -m pip install --no-cache-dir --upgrade pip && \
    python3 -m pip install --no-cache-dir neo4j

# Set PYSPARK_PYTHON to tell workers which Python to use
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3

# Set versions for dependencies (matching Spark 3.5.3's built-in Hadoop 3.3.4)
ENV HADOOP_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.367
ENV POSTGRES_JDBC_VERSION=42.7.3
ENV HIVE_VERSION=3.1.3

# Download required JAR files for S3A support, AWS, and Hive
ARG MAVEN_BASE=https://repo.maven.apache.org/maven2
RUN cd /opt/spark/jars && \
    # Hadoop AWS (S3A support)
    wget -q ${MAVEN_BASE}/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar && \
    # AWS SDK Bundle
    wget -q ${MAVEN_BASE}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar && \
    # PostgreSQL JDBC Driver for Hive Metastore
    wget -q ${MAVEN_BASE}/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar && \
    # Hive 3.1.3 JARs for metastore connectivity
    wget -q ${MAVEN_BASE}/org/apache/hive/hive-metastore/${HIVE_VERSION}/hive-metastore-${HIVE_VERSION}.jar && \
    wget -q ${MAVEN_BASE}/org/apache/hive/hive-exec/${HIVE_VERSION}/hive-exec-${HIVE_VERSION}-core.jar && \
    wget -q ${MAVEN_BASE}/org/apache/hive/hive-common/${HIVE_VERSION}/hive-common-${HIVE_VERSION}.jar && \
    wget -q ${MAVEN_BASE}/org/apache/hive/hive-serde/${HIVE_VERSION}/hive-serde-${HIVE_VERSION}.jar && \
    wget -q ${MAVEN_BASE}/org/apache/hive/hive-standalone-metastore/${HIVE_VERSION}/hive-standalone-metastore-${HIVE_VERSION}.jar && \
    wget -q ${MAVEN_BASE}/org/apache/thrift/libthrift/0.12.0/libthrift-0.12.0.jar

# Verify JARs were downloaded
RUN ls -lh /opt/spark/jars/hadoop-aws-* && \
    ls -lh /opt/spark/jars/aws-java-sdk-bundle-* && \
    ls -lh /opt/spark/jars/postgresql-* && \
    ls -lh /opt/spark/jars/hive-* && \
    ls -lh /opt/spark/jars/libthrift-*

USER spark

WORKDIR /opt/spark

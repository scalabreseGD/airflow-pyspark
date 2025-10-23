FROM jupyter/base-notebook:latest

USER root

# Install Java 11 (required for Spark 3.5.3)
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless wget procps && \
    rm -rf /var/lib/apt/lists/*

# Set Java home dynamically based on architecture
RUN JAVA_HOME_PATH=$(dirname $(dirname $(readlink -f $(which java)))) && \
    echo "export JAVA_HOME=${JAVA_HOME_PATH}" >> /etc/profile.d/java.sh
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64

# Set Spark version and download URL
ENV SPARK_VERSION=3.5.3
ENV HADOOP_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.367
ENV POSTGRES_JDBC_VERSION=42.7.3
ENV HIVE_VERSION=3.1.3

# Download and install Spark 3.5.3
RUN cd /tmp && \
    wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 /usr/local/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# Set Spark environment variables
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=jupyter
ENV PYSPARK_DRIVER_PYTHON_OPTS=notebook

# Download required JAR files for S3A support, AWS, and Hive
RUN cd /usr/local/spark/jars && \
    # Hadoop AWS (S3A support)
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar && \
    # AWS SDK Bundle
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar && \
    # PostgreSQL JDBC Driver for Hive Metastore
    wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar && \
    # Hive 3.1.3 JARs for metastore connectivity
    wget -q https://repo1.maven.org/maven2/org/apache/hive/hive-metastore/${HIVE_VERSION}/hive-metastore-${HIVE_VERSION}.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/hive/hive-exec/${HIVE_VERSION}/hive-exec-${HIVE_VERSION}-core.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/hive/hive-common/${HIVE_VERSION}/hive-common-${HIVE_VERSION}.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/hive/hive-serde/${HIVE_VERSION}/hive-serde-${HIVE_VERSION}.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/hive/hive-standalone-metastore/${HIVE_VERSION}/hive-standalone-metastore-${HIVE_VERSION}.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/thrift/libthrift/0.12.0/libthrift-0.12.0.jar

# Install PySpark and other Python packages
USER ${NB_UID}
RUN pip install --no-cache-dir \
    pyspark==${SPARK_VERSION} \
    pandas \
    pyarrow \
    matplotlib \
    seaborn

WORKDIR /home/jovyan

# Keep the original entrypoint and cmd from base image
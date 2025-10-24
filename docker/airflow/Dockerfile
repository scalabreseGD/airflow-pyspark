FROM apache/airflow:2.7.0

# Install Spark provider first as airflow user
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==4.1.3

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

# Download and install Spark 3.5.3 with proper extraction
RUN mkdir -p ${SPARK_HOME} && \
    curl -L https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz -o spark-3.5.3-bin-hadoop3.tgz && \
    tar xzf spark-3.5.3-bin-hadoop3.tgz --directory ${SPARK_HOME} --strip-components 1 && \
    rm -rf spark-3.5.3-bin-hadoop3.tgz

# Download additional JARs for S3/MinIO support
RUN curl -L -o ${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

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

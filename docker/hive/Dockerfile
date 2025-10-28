# syntax=docker/dockerfile:1.6

ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.367
ARG POSTGRES_JDBC_VERSION=42.7.3

FROM apache/hive:3.1.3

USER root

# Install curl in a single cached layer
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# Replace Hadoop with desired version
ARG HADOOP_VERSION
RUN mv /opt/hadoop /opt/hadoop-old && \
    cd /opt && \
    curl -fsSL -O https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} hadoop && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

# Add AWS and JDBC jars with versioned names for stable caching
ARG AWS_SDK_VERSION
ARG POSTGRES_JDBC_VERSION
RUN cd /opt/hive/lib && \
    curl -fsSL -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar && \
    rm -f postgresql-*.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar && \
    cp /opt/hive/lib/hadoop-aws-${HADOOP_VERSION}.jar ${HADOOP_HOME}/share/hadoop/common/lib/ && \
    cp /opt/hive/lib/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar ${HADOOP_HOME}/share/hadoop/common/lib/ && \
    cp /opt/hive/lib/postgresql-${POSTGRES_JDBC_VERSION}.jar ${HADOOP_HOME}/share/hadoop/common/lib/

USER hive

# syntax=docker/dockerfile:1.6

ARG SPARK_VERSION=3.5.3
ARG HADOOP_VERSION=2.10.1
ARG AWS_SDK_VERSION=1.12.367
ARG POSTGRES_JDBC_VERSION=42.7.3

FROM spark-with-jars:${SPARK_VERSION} AS spark-jars

FROM slamdev/apache-hive:2.3.9-2.10.1

USER root

# Install curl in a single cached layer (Alpine uses apk)
RUN apk add --no-cache curl

# Replace Hadoop with desired version
ARG HADOOP_VERSION
ARG AWS_SDK_VERSION
ARG POSTGRES_JDBC_VERSION
#RUN mv /opt/hadoop /opt/hadoop-old && \
#    cd /opt && \
#    curl -fsSL -O https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
#    tar xzf hadoop-${HADOOP_VERSION}.tar.gz && \
#    mv hadoop-${HADOOP_VERSION} hadoop && \
#    rm hadoop-${HADOOP_VERSION}.tar.gz

ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

# Download Hadoop 2.10.1 compatible JARs for Hive (not from Spark which uses 3.3.4)
RUN cd /opt/hive/lib && \
    curl -fsSL -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar

# Also copy to Hadoop common lib where needed
RUN cp /opt/hive/lib/hadoop-aws-${HADOOP_VERSION}.jar ${HADOOP_HOME}/share/hadoop/common/lib/ && \
    cp /opt/hive/lib/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar ${HADOOP_HOME}/share/hadoop/common/lib/ && \
    cp /opt/hive/lib/postgresql-${POSTGRES_JDBC_VERSION}.jar ${HADOOP_HOME}/share/hadoop/common/lib/

# Note: Staying as root user since the base image doesn't have a 'hive' user defined

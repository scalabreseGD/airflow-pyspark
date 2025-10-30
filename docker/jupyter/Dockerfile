# syntax=docker/dockerfile:1.6

ARG SPARK_VERSION=3.5.3
ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.367
ARG POSTGRES_JDBC_VERSION=42.7.3
ARG HIVE_VERSION=2.3.9

FROM apache/spark:${SPARK_VERSION} AS spark-dist

FROM jupyter/base-notebook:latest

ARG HADOOP_VERSION
ARG AWS_SDK_VERSION
ARG POSTGRES_JDBC_VERSION
ARG HIVE_VERSION

USER root

# Install Java and tools in a single cached layer
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jre-headless wget procps curl && \
    rm -rf /var/lib/apt/lists/*

# Normalize JAVA_HOME across architectures and ensure java is on PATH
RUN arch=$(dpkg --print-architecture) && \
    if [ "$arch" = "amd64" ]; then \
      ln -s /usr/lib/jvm/java-11-openjdk-amd64 /usr/lib/jvm/java-11-openjdk; \
    else \
      ln -s /usr/lib/jvm/java-11-openjdk-arm64 /usr/lib/jvm/java-11-openjdk; \
    fi
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk
ENV PATH=${JAVA_HOME}/bin:${PATH}

# Provide Spark from prebuilt image to avoid tarball downloads
ARG SPARK_VERSION
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
COPY --from=spark-dist /opt/spark ${SPARK_HOME}

# Python integration for PySpark without pip-installing pyspark
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=jupyter
ENV PYSPARK_DRIVER_PYTHON_OPTS=notebook

# Add S3 and JDBC JARs only; use Spark's built-in Hive 2.3 deps
RUN cd ${SPARK_HOME}/jars && \
    curl -fsSL -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar

# Install Python data-science packages with pip cache
USER ${NB_UID}
RUN --mount=type=cache,target=/home/jovyan/.cache/pip \
    pip install pandas pyarrow matplotlib seaborn

# Create IPython startup directory and copy startup script
RUN mkdir -p /home/jovyan/.ipython/profile_default/startup
COPY --chown=${NB_UID}:${NB_GID} startup/00-spark-init.py /home/jovyan/.ipython/profile_default/startup/

WORKDIR /home/jovyan
# Hive Metastore with S3A support and Hadoop 3.3.4
FROM apache/hive:3.1.3

USER root

# Set versions to match Spark's Hadoop version
ENV HADOOP_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.367
ENV POSTGRES_JDBC_VERSION=42.7.3

# Install curl
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Backup and remove old Hadoop, then install Hadoop 3.3.4
RUN mv /opt/hadoop /opt/hadoop-old && \
    cd /opt && \
    curl -L -O https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} hadoop && \
    rm hadoop-${HADOOP_VERSION}.tar.gz && \
    echo "Installed Hadoop ${HADOOP_VERSION}"

# Set Hadoop environment variables
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

# Download AWS SDK, Hadoop AWS, and updated PostgreSQL JDBC driver to Hive lib
RUN cd /opt/hive/lib && \
    curl -L -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar && \
    curl -L -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar && \
    rm -f postgresql-*.jar && \
    curl -L -O https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar && \
    echo "Downloaded AWS JARs and updated PostgreSQL JDBC driver"

# Copy JARs to Hadoop lib directory
RUN cp /opt/hive/lib/hadoop-aws-${HADOOP_VERSION}.jar ${HADOOP_HOME}/share/hadoop/common/lib/ && \
    cp /opt/hive/lib/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar ${HADOOP_HOME}/share/hadoop/common/lib/ && \
    cp /opt/hive/lib/postgresql-${POSTGRES_JDBC_VERSION}.jar ${HADOOP_HOME}/share/hadoop/common/lib/ && \
    echo "Copied JARs to Hadoop lib directory"

# Verify JARs are present
RUN ls -lh /opt/hive/lib/hadoop-aws-*.jar && \
    ls -lh /opt/hive/lib/aws-java-sdk-bundle-*.jar && \
    ls -lh /opt/hive/lib/postgresql-*.jar

USER hive

FROM apache/hive:3.1.3

USER root

# Install curl and netcat for downloading JARs and healthchecks
RUN apt-get update && apt-get install -y curl netcat && rm -rf /var/lib/apt/lists/*

# Download PostgreSQL JDBC driver
RUN curl -fSL https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
    -o /opt/hive/lib/postgresql-42.7.3.jar

# Note: S3A JARs not needed in Hive Metastore - it only stores metadata in PostgreSQL
# Spark handles all S3A operations for data files

# Verify PostgreSQL JAR was downloaded
RUN ls -lh /opt/hive/lib/postgresql-42.7.3.jar

USER hive

WORKDIR /opt/hive

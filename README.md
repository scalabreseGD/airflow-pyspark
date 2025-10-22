# Spark + MinIO + Hive Integration Stack

A fully functional, production-ready Docker Compose setup for Apache Spark with Hive Metastore and MinIO (S3-compatible) storage.

## Features

- **Apache Spark 3.5.3** with master and worker nodes
- **Hive Metastore 4.0.0** for metadata management
- **MinIO** as S3-compatible object storage
- **PostgreSQL 14** as Hive Metastore backend
- Full S3A protocol support for direct MinIO writes/reads
- Complete Spark SQL integration with Hive tables
- Pre-configured with all necessary JAR dependencies
- Health checks and automatic service orchestration

## Architecture

```
┌─────────────────┐
│  Spark Master   │ ←→ Spark Worker
│   + Worker      │     (More workers can be added)
└────────┬────────┘
         │
         ├─────────→ MinIO (S3-compatible storage)
         │           • s3a://warehouse/ (Hive tables)
         │           • s3a://data/ (Direct writes)
         │
         └─────────→ Hive Metastore
                     └─→ PostgreSQL (Metadata DB)
```

## Quick Start

### 1. Start the Stack

```bash
./start.sh
```

This will:
- Build custom Spark images with all required dependencies
- Start all services (MinIO, PostgreSQL, Hive, Spark)
- Wait for services to become healthy
- Display service URLs

### 2. Run the Test Suite

```bash
./test.sh
```

This comprehensive test validates:
- Direct S3A writes to MinIO
- Direct S3A reads from MinIO
- Hive database and table creation
- Managed Hive tables with MinIO storage
- External Hive tables with custom S3A locations
- Spark SQL queries on Hive tables
- DataFrame API with Hive integration
- Multiple file formats (Parquet, CSV)
- Partitioned tables

### 3. Submit Your Own Spark Jobs

```bash
./submit.sh your_script.py
```

Place your Python scripts in the `spark-apps/` directory.

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Spark Master UI | http://localhost:8080 | - |
| Spark Worker UI | http://localhost:8081 | - |
| Spark Job UI | http://localhost:4040 | (When job is running) |
| MinIO Console | http://localhost:9001 | admin / admin123 |
| MinIO API | http://localhost:9000 | admin / admin123 |
| Hive Metastore | thrift://localhost:9083 | - |
| PostgreSQL | localhost:5432 | hive / hive123 |

## Writing Spark Applications

### Basic Template

```python
from pyspark.sql import SparkSession

# Create Spark session with Hive and MinIO support
spark = SparkSession.builder \
    .appName("MyApp") \
    .enableHiveSupport() \
    .getOrCreate()

# Your code here...

spark.stop()
```

### Direct MinIO Write/Read

```python
# Write to MinIO using S3A protocol
df.write.mode("overwrite").parquet("s3a://data/my-dataset/")

# Read from MinIO
df = spark.read.parquet("s3a://data/my-dataset/")
```

### Hive Tables

```python
# Create database
spark.sql("CREATE DATABASE IF NOT EXISTS my_db")
spark.sql("USE my_db")

# Create managed table (stored in s3a://warehouse/)
df.write.mode("overwrite").saveAsTable("my_table")

# Query with Spark SQL
result = spark.sql("SELECT * FROM my_table WHERE age > 30")
result.show()

# Create external table with custom location
spark.sql(f"""
    CREATE EXTERNAL TABLE my_external_table (
        id INT,
        name STRING,
        value DOUBLE
    )
    STORED AS PARQUET
    LOCATION 's3a://data/my-external-data/'
""")
```

### Partitioned Tables

```python
# Write partitioned table
df.write.mode("overwrite") \
    .partitionBy("department", "year") \
    .saveAsTable("employees_partitioned")

# Query specific partition
spark.sql("""
    SELECT * FROM employees_partitioned
    WHERE department = 'Engineering' AND year = 2024
""").show()
```

## Configuration

### MinIO Buckets

Two buckets are automatically created:
- `warehouse` - Default location for Hive managed tables
- `data` - For general data storage and external tables

### S3A Configuration

All S3A settings are pre-configured in:
- `conf/spark/hive-site.xml` - Spark/Hive S3 configuration
- `conf/spark/spark-defaults.conf` - Spark defaults
- `conf/hive/metastore-site.xml` - Hive Metastore S3 configuration
- `conf/hive/core-site.xml` - Hadoop core S3 configuration

### Key Settings

```properties
# MinIO endpoint (internal Docker network)
fs.s3a.endpoint = http://minio:9000

# Credentials
fs.s3a.access.key = admin
fs.s3a.secret.key = admin123

# Path style access (required for MinIO)
fs.s3a.path.style.access = true

# Disable SSL (for local development)
fs.s3a.connection.ssl.enabled = false
```

## Managing the Stack

### Start Services

```bash
./start.sh
```

### Clean Start (Remove all data)

```bash
./start.sh --clean
```

### Stop Services

```bash
docker-compose down
```

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f spark-master
docker-compose logs -f hive-metastore
docker-compose logs -f minio
```

### Check Service Status

```bash
docker-compose ps
```

### Access MinIO Console

1. Open http://localhost:9001
2. Login with `admin` / `admin123`
3. Browse buckets and objects

### Access Spark Shell

```bash
docker exec -it spark-master /opt/spark/bin/spark-shell \
    --conf spark.sql.warehouse.dir=s3a://warehouse/ \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000
```

### Access PySpark Shell

```bash
docker exec -it spark-master /opt/spark/bin/pyspark \
    --conf spark.sql.warehouse.dir=s3a://warehouse/ \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000
```

## Troubleshooting

### Services Not Starting

Check logs for specific service:
```bash
docker-compose logs hive-metastore
docker-compose logs spark-master
```

### Connection Issues

Verify all services are healthy:
```bash
docker-compose ps
```

All services should show "healthy" or "Up" status.

### S3A Connection Errors

1. Verify MinIO is running: http://localhost:9001
2. Check MinIO credentials in configuration files
3. Ensure buckets exist (created automatically by minio-setup)

### Hive Metastore Issues

1. Check PostgreSQL is running and healthy
2. Verify Hive Metastore logs:
   ```bash
   docker-compose logs hive-metastore
   ```
3. Check metastore connection:
   ```bash
   docker exec hive-metastore-db psql -U hive -d metastore -c "SELECT * FROM VERSION;"
   ```

### Performance Tuning

Edit `conf/spark/spark-defaults.conf` to adjust:
- `spark.executor.memory`
- `spark.executor.cores`
- `spark.hadoop.fs.s3a.connection.maximum`
- `spark.hadoop.fs.s3a.threads.max`

## Project Structure

```
.
├── docker-compose.yml              # Main orchestration file
├── docker/
│   └── spark/
│       └── Dockerfile              # Custom Spark image with S3A JARs
├── conf/
│   ├── hive/
│   │   ├── metastore-site.xml     # Hive Metastore configuration
│   │   └── core-site.xml          # Hadoop core configuration
│   └── spark/
│       ├── hive-site.xml          # Spark Hive configuration
│       └── spark-defaults.conf    # Spark default settings
├── spark-apps/
│   └── complete_test.py           # Comprehensive test suite
├── start.sh                       # Startup script
├── submit.sh                      # Job submission script
└── test.sh                        # Test runner script
```

## Dependencies Included

The custom Spark image includes:
- `hadoop-aws-3.3.4.jar` - S3A filesystem support
- `aws-java-sdk-bundle-1.12.367.jar` - AWS SDK for S3 operations
- `postgresql-42.7.3.jar` - PostgreSQL JDBC driver

All dependencies are automatically downloaded during image build.

## Advanced Usage

### Adding More Workers

Edit `docker-compose.yml` and add additional worker services:

```yaml
spark-worker-2:
  build:
    context: ./docker/spark
    dockerfile: Dockerfile
  container_name: spark-worker-2
  command: /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
  # ... rest of configuration
```

### Custom Spark Configuration

Edit `conf/spark/spark-defaults.conf` to add custom settings.

### External Access

To allow external applications to connect:
1. Update service names to use `localhost` instead of internal names
2. Adjust port mappings in `docker-compose.yml`
3. Update configuration files with external endpoints

## Support and Documentation

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Hive Documentation](https://hive.apache.org/)
- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)

## License

This project is provided as-is for educational and development purposes.

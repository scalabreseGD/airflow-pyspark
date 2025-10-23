# Spark + MinIO + Hive Integration Stack

A fully functional, production-ready Docker Compose setup for Apache Spark with Hive Metastore and MinIO (S3-compatible) storage.

## Features

- **Apache Spark 3.5.3** with master and worker nodes
- **Hive Metastore 3.1.3** with **Hadoop 3.3.4** for metadata management
- **MinIO** as S3-compatible object storage
- **PostgreSQL 14** as Hive Metastore backend
- Full S3A protocol support for direct MinIO writes/reads
- Complete Spark SQL integration with Hive tables
- **Hive External Tables** - table metadata in Hive, data in MinIO
- **SparkSQL** examples with temporary views and complex queries
- Pre-configured with all necessary JAR dependencies (AWS SDK, Hadoop AWS, PostgreSQL JDBC)
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

### 3. Explore Example Spark Jobs

Try the included example jobs:

```bash
# Basic data processing - create sample data in MinIO
./submit.sh create_sample_data.py

# Filter and transform data with DataFrames
./submit.sh filter_employees.py

# SparkSQL with temporary views and complex queries
./submit.sh sql_analysis.py

# Hive External Tables - metadata in Hive, data in MinIO
./submit.sh hive_external_tables.py
```

### 4. Submit Your Own Spark Jobs

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

### Using SparkSQL with Temporary Views

```python
# Read data from MinIO
employees = spark.read.parquet("s3a://data/employees/")

# Create temporary SQL view
employees.createOrReplaceTempView("employees")

# Run SQL queries with CASE statements, aggregations, etc.
result = spark.sql("""
    SELECT
        name,
        salary,
        CASE
            WHEN salary >= 100000 THEN 'Senior'
            WHEN salary >= 90000 THEN 'Mid-Level'
            ELSE 'Junior'
        END as performance_tier
    FROM employees
    WHERE department = 'Engineering'
    ORDER BY salary DESC
""")

# Aggregations with GROUP BY
dept_stats = spark.sql("""
    SELECT
        department,
        COUNT(*) as employee_count,
        ROUND(AVG(salary), 2) as avg_salary,
        MIN(salary) as min_salary,
        MAX(salary) as max_salary
    FROM employees
    GROUP BY department
    ORDER BY avg_salary DESC
""")
```

See `spark-apps/sql_analysis.py` for a complete example.

### Hive External Tables - Best of Both Worlds

External tables provide persistent table definitions in Hive Metastore while keeping data in MinIO. This allows you to:
- Query tables by name instead of full S3A paths
- Keep data in MinIO (survives even if you DROP TABLE)
- Share table definitions across multiple Spark jobs
- Use catalog features (SHOW TABLES, DESCRIBE, etc.)

```python
# Create database
spark.sql("CREATE DATABASE IF NOT EXISTS analytics")
spark.sql("USE analytics")

# Create external table pointing to existing data in MinIO
spark.sql("""
    CREATE EXTERNAL TABLE IF NOT EXISTS employees (
        id INT,
        name STRING,
        age INT,
        department STRING,
        salary INT
    )
    STORED AS PARQUET
    LOCATION 's3a://data/employees/'
""")

# Now query by table name (no need for full S3A paths!)
spark.sql("SELECT * FROM employees WHERE salary >= 85000").show()

# Tables persist across sessions
spark.sql("SHOW TABLES").show()
spark.sql("DESCRIBE EXTENDED employees").show()
```

**Important Notes:**
- External tables store metadata in Hive, data stays in MinIO at the specified LOCATION
- The `warehouse/analytics.db/` directory will be empty (expected behavior)
- Data is stored at the LOCATION path (`s3a://data/employees/`)
- Dropping external tables only removes metadata, not data

See `spark-apps/hive_external_tables.py` for a comprehensive example.

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
│   ├── spark/
│   │   └── Dockerfile              # Custom Spark image with S3A JARs
│   └── hive/
│       └── Dockerfile              # Custom Hive Metastore with Hadoop 3.3.4
├── conf/
│   ├── hive/
│   │   ├── metastore-site.xml     # Hive Metastore configuration
│   │   └── core-site.xml          # Hadoop core S3A configuration
│   └── spark/
│       ├── hive-site.xml          # Spark Hive configuration
│       └── spark-defaults.conf    # Spark default settings
├── spark-apps/
│   ├── create_sample_data.py      # Create sample datasets
│   ├── filter_employees.py        # DataFrame filtering example
│   ├── sql_analysis.py            # SparkSQL with temporary views
│   ├── hive_external_tables.py    # Hive external tables demo
│   └── complete_test.py           # Comprehensive test suite
├── start.sh                       # Startup script
├── submit.sh                      # Job submission script
└── test.sh                        # Test runner script
```

## Dependencies Included

### Custom Spark Image
- `hadoop-aws-3.3.4.jar` - S3A filesystem support
- `aws-java-sdk-bundle-1.12.367.jar` - AWS SDK for S3 operations
- `postgresql-42.7.3.jar` - PostgreSQL JDBC driver

### Custom Hive Metastore Image
- **Hadoop 3.3.4** - Full Hadoop installation (upgraded from bundled 3.1.x)
- `hadoop-aws-3.3.4.jar` - S3A filesystem support for Hive
- `aws-java-sdk-bundle-1.12.367.jar` - AWS SDK for MinIO access
- `postgresql-42.7.3.jar` - PostgreSQL JDBC driver with SCRAM-SHA-256 support

All dependencies are automatically downloaded during image build. The Hive Metastore uses a custom Dockerfile that replaces the bundled Hadoop 3.1.x with Hadoop 3.3.4 for compatibility with modern AWS libraries.

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

## Frequently Asked Questions

### Why is `warehouse/analytics.db/` empty in MinIO?

This is **expected behavior** for external tables. When you create an external table with a `LOCATION` clause:
- Table **metadata** is stored in Hive Metastore
- Table **data** is stored at the specified `LOCATION` (e.g., `s3a://data/employees/`)
- The `warehouse/analytics.db/` directory is just a placeholder for the database

To see data in the `warehouse` bucket, create a **managed table** instead:
```python
df.write.mode("overwrite").saveAsTable("my_managed_table")
```

### What's the difference between External and Managed tables?

**External Tables:**
- Metadata in Hive, data at custom LOCATION
- `DROP TABLE` only removes metadata, not data
- Data survives table deletion
- Use for shared data or data you don't want Hive to manage

**Managed Tables:**
- Both metadata and data managed by Hive
- Data stored in `s3a://warehouse/database.db/table_name/`
- `DROP TABLE` removes both metadata and data
- Use for temporary or Hive-exclusive data

### How do I verify my Hive tables?

```bash
# Connect to Spark and check tables
docker exec -it spark-master /opt/spark/bin/pyspark

# In PySpark:
spark.sql("SHOW DATABASES").show()
spark.sql("USE analytics")
spark.sql("SHOW TABLES").show()
spark.sql("DESCRIBE EXTENDED employees").show()
```

### How do I check data in MinIO?

```bash
# List data bucket contents
docker exec minio-setup mc ls myminio/data/ --recursive

# List warehouse bucket contents
docker exec minio-setup mc ls myminio/warehouse/ --recursive
```

Or use the MinIO Console at http://localhost:9001 (admin/admin123)

## Support and Documentation

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Hive Documentation](https://hive.apache.org/)
- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)

## License

This project is provided as-is for educational and development purposes.

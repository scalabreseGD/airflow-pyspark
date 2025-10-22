# Quick Start Guide

## What I've Built For You

A complete, working docker-compose stack with:

1. **Spark 3.5.3** with custom-built images including all S3A/Hive JARs
2. **Hive Metastore 4.0.0** with PostgreSQL backend
3. **MinIO** for S3-compatible storage
4. **Complete configuration** - all XML and conf files properly set up
5. **Comprehensive test suite** - validates the entire stack

## Start the Stack

```bash
./start.sh
```

This will:
- Build Spark images with all dependencies (first time only)
- Start all services with health checks
- Show you when everything is ready
- Display all service URLs

## Run the Complete Test

```bash
./test.sh
```

This comprehensive test will validate:
- Direct writes to MinIO via S3A protocol
- Direct reads from MinIO
- Hive database and table creation
- Managed tables (stored in MinIO warehouse bucket)
- External tables (custom S3A locations)
- Spark SQL queries
- DataFrame API with Hive
- Multiple formats (Parquet, CSV)
- Partitioned tables

## What Changed From Your Old Setup

### Problems Fixed

1. **Missing JARs**: Added hadoop-aws, aws-java-sdk-bundle, postgresql jars
2. **Configuration**: Proper S3A settings in all config files
3. **Health checks**: Services wait for dependencies
4. **Hive version**: Updated from 3.1.3 to 4.0.0 for better compatibility

### New Structure

```
airflow-pyspark/
├── docker-compose.yml          # Clean orchestration
├── docker/spark/Dockerfile     # Custom Spark with all JARs
├── conf/
│   ├── hive/                   # Hive Metastore config
│   └── spark/                  # Spark config
├── spark-apps/
│   └── complete_test.py        # Full integration test
├── start.sh                    # Easy startup
├── submit.sh                   # Submit jobs
└── test.sh                     # Run tests
```

## Service URLs

- **Spark Master UI**: http://localhost:8080
- **MinIO Console**: http://localhost:9001 (admin/admin123)
- **Spark Job UI**: http://localhost:4040 (when running)

## Writing Your Own Scripts

Place Python files in `spark-apps/` and run:

```bash
./submit.sh your_script.py
```

### Example Template

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .enableHiveSupport() \
    .getOrCreate()

# Direct MinIO write
df.write.parquet("s3a://data/my-data/")

# Hive table
df.write.saveAsTable("my_table")

# Spark SQL
result = spark.sql("SELECT * FROM my_table WHERE age > 30")
result.show()

spark.stop()
```

## Clean Restart

To start fresh (removes all data):

```bash
./start.sh --clean
```

## Troubleshooting

### Check logs
```bash
docker-compose logs -f [service_name]
```

### Check status
```bash
docker-compose ps
```

### All services should show "healthy" or "Up"

## Next Steps

1. Start the stack: `./start.sh`
2. Run the test: `./test.sh`
3. Check MinIO console to see your data
4. Write your own scripts in `spark-apps/`
5. Submit with `./submit.sh your_script.py`

The confusion is over. Everything works now.

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
- Managed tables (stored in local warehouse)
- Spark SQL queries
- DataFrame API with Hive
- Multiple formats (Parquet, CSV)
- Partitioned tables

## Try the Example ETL Job

After running the test, try the example ETL job workflow:

### Step 1: Create Sample Data

```bash
./submit.sh create_sample_data.py
```

This creates employee data in MinIO at `s3a://data/employees/`

### Step 2: Run the ETL Job

```bash
./submit.sh filter_employees.py
```

This example job demonstrates:
1. Reading from MinIO via S3A (`s3a://data/employees/`)
2. Filtering data (Engineering dept, salary >= $85k)
3. Adding a computed column (`performance_tier`)
4. Writing results back to MinIO (`s3a://data/high_performers/`)
5. Generating summary statistics

**What it does:**
- Reads employee data from MinIO
- Filters high-performing Engineering employees
- Writes enriched data back to MinIO
- Shows how multiple jobs can work with the same data in MinIO

**View Results:**
- Open MinIO Console: http://localhost:9001 (admin/admin123)
- Navigate to the `data` bucket
- You'll see `employees/` and `high_performers/` directories with Parquet files

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

## Writing Your Own Spark Jobs

### Quick Start: Submit a Job

1. **Create your Python script** in the `spark-apps/` directory
2. **Submit the job** using the submit script:

```bash
./submit.sh your_script.py
```

### Example Jobs Provided

- **`complete_test.py`**: Comprehensive integration test
- **`create_sample_data.py`**: Creates sample employee data in MinIO
- **`filter_employees.py`**: Example ETL job (reads from MinIO, filters, writes back to MinIO)

### Job Template

Here's a basic template for your Spark jobs (using direct MinIO access via S3A):

```python
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()

try:
    # Read from MinIO via S3A
    df = spark.read.parquet("s3a://data/input/")

    # Transform your data
    filtered = df.filter(df.age > 30)

    # Write back to MinIO
    filtered.write.mode("overwrite").parquet("s3a://data/output/")

    # Verify
    result = spark.read.parquet("s3a://data/output/")
    print(f"Records written: {result.count()}")
    result.show()

finally:
    spark.stop()
```

**Note**: This template uses direct S3A paths (recommended approach). All data is stored in MinIO and can be viewed in the MinIO Console.

### Common Patterns

#### Pattern 1: ETL Pipeline (Read → Transform → Write)

```python
# Read from MinIO
source_df = spark.read.parquet("s3a://data/raw_data/")

# Transform
transformed = source_df \
    .filter(source_df.status == "active") \
    .select("id", "name", "value")

# Write to MinIO
transformed.write.mode("overwrite").parquet("s3a://data/processed_data/")
```

#### Pattern 2: Data Aggregation

```python
# Read and aggregate
df = spark.read.parquet("s3a://data/sales/")
summary = df.groupBy("department") \
    .agg({"amount": "sum", "quantity": "avg"})

# Save results
summary.write.mode("overwrite").parquet("s3a://data/sales_summary/")
```

#### Pattern 3: Multiple Source Joins

```python
# Read from multiple sources
employees = spark.read.parquet("s3a://data/employees/")
departments = spark.read.parquet("s3a://data/departments/")

# Join and write result
result = employees.join(departments, "dept_id") \
    .select("emp_name", "dept_name", "salary")

result.write.mode("overwrite").parquet("s3a://data/employee_dept_view/")
```

### Important Notes

1. **Use S3A paths** for all data access: `s3a://bucket/path/`
2. **Data is stored in MinIO** - view it in the MinIO Console (http://localhost:9001)
3. **Multiple jobs can work with the same data** - all data persists in MinIO
4. **Use `spark.stop()`** in a `finally` block to ensure cleanup
5. **Parquet format** is recommended for efficiency (columnar, compressed)
6. **Hive Metastore is available** for advanced use cases, but direct S3A is simpler

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

# Airflow + PySpark Bronze/Silver/Gold Integration

Complete guide for orchestrating Bronze/Silver/Gold data lake ETL pipelines using Apache Airflow and Apache Spark.

## Overview

This setup integrates Apache Airflow 2.7.0 with PySpark to orchestrate the Bronze/Silver/Gold (Medallion Architecture) ETL pipeline using the `SparkSubmitOperator`.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   Airflow Orchestration                      │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  Airflow Scheduler                                           │
│       ↓                                                       │
│  [Bronze DAG] → SparkSubmitOperator → Spark Master          │
│       ↓                                    ↓                 │
│  [Silver DAG] → SparkSubmitOperator → Spark Worker(s)       │
│       ↓                                    ↓                 │
│  [Gold DAG]   → SparkSubmitOperator → MinIO (S3A)           │
│                                            ↓                 │
│                                       Hive Metastore         │
└─────────────────────────────────────────────────────────────┘
```

**Components:**
- **Airflow Scheduler**: Triggers and monitors DAGs
- **Airflow Webserver**: UI for managing workflows
- **SparkSubmitOperator**: Submits PySpark jobs to Spark cluster
- **Spark Master**: Receives and schedules Spark jobs
- **Spark Workers**: Execute transformations
- **MinIO**: Stores bronze/silver/gold data
- **Hive Metastore**: Manages table metadata
- **PostgreSQL**: Backend for Airflow and Hive

## Why SparkSubmitOperator?

The `SparkSubmitOperator` is the recommended way to run Spark jobs from Airflow:

1. **Native Integration**: Built specifically for Spark-Airflow integration
2. **Connection Management**: Centralized Spark connection configuration
3. **Better Logging**: Full integration with Airflow's logging system
4. **Error Handling**: Proper task failure detection and retry logic
5. **Monitoring**: Track Spark job status through Airflow UI
6. **Configuration**: Pass Spark configs dynamically per job

## Setup Instructions

### 1. Start All Services

```bash
# Build custom Airflow image with Spark support
docker-compose build

# Start all services
docker-compose up -d

# Verify services are running
docker-compose ps
```

All services should show "healthy" or "Up" status.

### 2. Configure Spark Connection

Run the setup script to create the Spark connection in Airflow:

```bash
./setup_airflow_connections.sh
```

**What this creates:**
- **Connection ID**: `spark_default`
- **Type**: Spark
- **Host**: `spark://spark-master`
- **Port**: `7077`
- **Deploy Mode**: client

**Verify in Airflow UI:**
1. Open: http://localhost:8082 (admin / admin)
2. Go to: Admin > Connections
3. Find: `spark_default` connection

### 3. Create Bronze/Silver/Gold Tables

Before running DAGs, create the table schemas using Jupyter notebooks:

```bash
# Get Jupyter token
docker-compose logs jupyter | grep token

# Open: http://localhost:8888
```

Run these notebooks in order:
1. `notebooks/create_bronze_tables.ipynb`
2. `notebooks/create_silver_tables.ipynb`
3. `notebooks/create_gold_tables.ipynb`

### 4. Verify Setup

**Check Airflow:**
- Open: http://localhost:8082 (admin / admin)
- Verify `ingest_bronze_data` DAG appears
- Check `spark_default` connection in Admin > Connections

**Check Spark:**
- Open: http://localhost:8080
- Verify Spark Master is running
- Check worker nodes are connected

## Existing DAGs

### Bronze Data Ingestion DAG

**File**: `dags/ingest_bronze_data_dag.py`

**Purpose**: Ingests raw CSV data into bronze layer tables

**Tasks:**
1. **check_prerequisites**: Validates environment is ready
2. **ingest_bronze_data**: Submits `ingest_bronze_data.py` to Spark
3. **verify_ingestion**: Validates ingestion completed successfully

**To run manually:**
1. Open Airflow: http://localhost:8082
2. Find `ingest_bronze_data` DAG
3. Toggle it to "ON" (unpause)
4. Click "Play" button and select "Trigger DAG"

**To schedule:**
Edit `dags/ingest_bronze_data_dag.py`:
```python
schedule_interval='@daily'  # Run daily at midnight
# or
schedule_interval='0 2 * * *'  # Run at 2 AM daily
```

**Configuration:**
The DAG passes these Spark configs:
```python
conf={
    # Hive Metastore
    'spark.sql.hive.metastore.version': '3.1.3',
    'spark.sql.catalogImplementation': 'hive',
    'spark.hadoop.hive.metastore.uris': 'thrift://hive-metastore:9083',
    'spark.sql.warehouse.dir': 's3a://warehouse/',

    # MinIO / S3A
    'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
    'spark.hadoop.fs.s3a.access.key': 'admin',
    'spark.hadoop.fs.s3a.secret.key': 'admin123',
    'spark.hadoop.fs.s3a.path.style.access': 'true',
    'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
}
```

## Creating Your Own DAGs

### Example: Silver Layer Processing DAG

Create: `dags/process_silver_dag.py`

```python
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Standard Spark configuration for all jobs
SPARK_CONFIG = {
    'spark.sql.hive.metastore.version': '3.1.3',
    'spark.sql.catalogImplementation': 'hive',
    'spark.hadoop.hive.metastore.uris': 'thrift://hive-metastore:9083',
    'spark.sql.warehouse.dir': 's3a://warehouse/',
    'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
    'spark.hadoop.fs.s3a.access.key': 'admin',
    'spark.hadoop.fs.s3a.secret.key': 'admin123',
    'spark.hadoop.fs.s3a.path.style.access': 'true',
    'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
}

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'process_silver_layer',
    default_args=default_args,
    description='Transform bronze data to silver layer',
    schedule_interval='@daily',
    catchup=False,
    tags=['silver', 'etl', 'transformation'],
) as dag:

    def check_bronze_data(**context):
        """Verify bronze data exists before processing"""
        print("Checking bronze layer data availability...")
        # Add your validation logic here
        return True

    check_bronze = PythonOperator(
        task_id='check_bronze_data',
        python_callable=check_bronze_data,
    )

    # Process transactions
    process_transactions = SparkSubmitOperator(
        task_id='process_transactions',
        application='/opt/spark-apps/silver_transactions.py',
        conn_id='spark_default',
        conf=SPARK_CONFIG,
        verbose=True,
    )

    # Process subscriptions
    process_subscriptions = SparkSubmitOperator(
        task_id='process_subscriptions',
        application='/opt/spark-apps/silver_subscriptions.py',
        conn_id='spark_default',
        conf=SPARK_CONFIG,
        verbose=True,
    )

    # Process customers
    process_customers = SparkSubmitOperator(
        task_id='process_customers',
        application='/opt/spark-apps/silver_customers.py',
        conn_id='spark_default',
        conf=SPARK_CONFIG,
        verbose=True,
    )

    def verify_silver_data(**context):
        """Verify silver data quality"""
        print("Verifying silver layer data quality...")
        # Add your validation logic here
        return True

    verify_silver = PythonOperator(
        task_id='verify_silver_data',
        python_callable=verify_silver_data,
    )

    # Define dependencies
    check_bronze >> [process_transactions, process_subscriptions, process_customers] >> verify_silver
```

### Example: Gold Layer Aggregation DAG

Create: `dags/process_gold_dag.py`

```python
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

SPARK_CONFIG = {
    'spark.sql.hive.metastore.version': '3.1.3',
    'spark.sql.catalogImplementation': 'hive',
    'spark.hadoop.hive.metastore.uris': 'thrift://hive-metastore:9083',
    'spark.sql.warehouse.dir': 's3a://warehouse/',
    'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
    'spark.hadoop.fs.s3a.access.key': 'admin',
    'spark.hadoop.fs.s3a.secret.key': 'admin123',
    'spark.hadoop.fs.s3a.path.style.access': 'true',
    'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
}

with DAG(
    'process_gold_layer',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['gold', 'aggregation', 'metrics'],
) as dag:

    customer_summary = SparkSubmitOperator(
        task_id='customer_summary',
        application='/opt/spark-apps/gold_customer_summary.py',
        conn_id='spark_default',
        conf=SPARK_CONFIG,
    )

    product_performance = SparkSubmitOperator(
        task_id='product_performance',
        application='/opt/spark-apps/gold_product_performance.py',
        conn_id='spark_default',
        conf=SPARK_CONFIG,
    )

    sales_metrics = SparkSubmitOperator(
        task_id='sales_metrics',
        application='/opt/spark-apps/gold_sales_metrics.py',
        conn_id='spark_default',
        conf=SPARK_CONFIG,
    )

    # All can run in parallel since they read from silver
    [customer_summary, product_performance, sales_metrics]
```

### Example: Full ETL Pipeline DAG

Create: `dags/full_etl_pipeline.py`

```python
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

SPARK_CONFIG = {
    'spark.sql.hive.metastore.version': '3.1.3',
    'spark.sql.catalogImplementation': 'hive',
    'spark.hadoop.hive.metastore.uris': 'thrift://hive-metastore:9083',
    'spark.sql.warehouse.dir': 's3a://warehouse/',
    'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
    'spark.hadoop.fs.s3a.access.key': 'admin',
    'spark.hadoop.fs.s3a.secret.key': 'admin123',
    'spark.hadoop.fs.s3a.path.style.access': 'true',
    'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
}

with DAG(
    'full_etl_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'pipeline', 'bronze-silver-gold'],
) as dag:

    # Bronze layer ingestion
    ingest_bronze = SparkSubmitOperator(
        task_id='ingest_bronze',
        application='/opt/spark-apps/ingest_bronze_data.py',
        conn_id='spark_default',
        conf=SPARK_CONFIG,
    )

    # Silver layer transformations (run in parallel)
    process_transactions = SparkSubmitOperator(
        task_id='silver_transactions',
        application='/opt/spark-apps/silver_transactions.py',
        conn_id='spark_default',
        conf=SPARK_CONFIG,
    )

    process_customers = SparkSubmitOperator(
        task_id='silver_customers',
        application='/opt/spark-apps/silver_customers.py',
        conn_id='spark_default',
        conf=SPARK_CONFIG,
    )

    # Gold layer aggregations (run in parallel)
    customer_summary = SparkSubmitOperator(
        task_id='gold_customer_summary',
        application='/opt/spark-apps/gold_customer_summary.py',
        conn_id='spark_default',
        conf=SPARK_CONFIG,
    )

    sales_metrics = SparkSubmitOperator(
        task_id='gold_sales_metrics',
        application='/opt/spark-apps/gold_sales_metrics.py',
        conn_id='spark_default',
        conf=SPARK_CONFIG,
    )

    # Define pipeline: bronze -> silver (parallel) -> gold (parallel)
    ingest_bronze >> [process_transactions, process_customers]
    [process_transactions, process_customers] >> [customer_summary, sales_metrics]
```

## SparkSubmitOperator Parameters

### Required Parameters

- **`task_id`**: Unique identifier for the task
- **`application`**: Path to the Python/Scala/Java application
- **`conn_id`**: Airflow connection ID (use `spark_default`)

### Common Optional Parameters

- **`conf`**: Dictionary of Spark configuration properties
- **`verbose`**: Enable verbose logging (default: True)
- **`name`**: Spark application name (overrides default)
- **`driver_memory`**: Memory for driver (e.g., "2g")
- **`executor_memory`**: Memory per executor (e.g., "4g")
- **`executor_cores`**: Cores per executor
- **`num_executors`**: Number of executors
- **`py_files`**: Additional Python files (comma-separated)
- **`files`**: Additional files to ship with the job
- **`packages`**: Maven packages (e.g., "org.postgresql:postgresql:42.7.3")

### Example with All Parameters

```python
SparkSubmitOperator(
    task_id='advanced_processing',
    application='/opt/spark-apps/my_job.py',
    conn_id='spark_default',
    name='AdvancedProcessing',
    conf={
        'spark.sql.hive.metastore.uris': 'thrift://hive-metastore:9083',
        'spark.sql.warehouse.dir': 's3a://warehouse/',
        # Add all S3A configs...
    },
    driver_memory='2g',
    executor_memory='4g',
    executor_cores=2,
    num_executors=2,
    py_files='/opt/spark-apps/utils.py,/opt/spark-apps/common.py',
    files='/opt/config/settings.json',
    verbose=True,
)
```

## Managing DAGs

### View All DAGs

```bash
# List all DAGs
docker-compose exec airflow-scheduler airflow dags list

# Get DAG details
docker-compose exec airflow-scheduler airflow dags show ingest_bronze_data
```

### Test a DAG

```bash
# Test entire DAG (dry run)
docker-compose exec airflow-scheduler airflow dags test ingest_bronze_data 2024-01-01

# Test specific task
docker-compose exec airflow-scheduler airflow tasks test ingest_bronze_data ingest_bronze_data 2024-01-01
```

### Trigger DAG Manually

```bash
# Via CLI
docker-compose exec airflow-scheduler airflow dags trigger ingest_bronze_data

# With config
docker-compose exec airflow-scheduler airflow dags trigger ingest_bronze_data --conf '{"param":"value"}'
```

### Pause/Unpause DAG

```bash
# Pause
docker-compose exec airflow-scheduler airflow dags pause ingest_bronze_data

# Unpause
docker-compose exec airflow-scheduler airflow dags unpause ingest_bronze_data
```

## Useful Commands

### Airflow Commands

```bash
# View logs
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver

# Check connections
docker-compose exec airflow-scheduler airflow connections list

# Export connection
docker-compose exec airflow-scheduler airflow connections export connections.json

# Restart services
docker-compose restart airflow-scheduler airflow-webserver
```

### Spark Job Monitoring

```bash
# View Spark Master UI
open http://localhost:8080

# View Spark job logs
docker-compose logs -f spark-master
docker-compose logs -f spark-worker

# Check running applications
curl http://localhost:8080/json/ | jq
```

## Service Ports

| Service | URL | Purpose |
|---------|-----|---------|
| Airflow Web UI | http://localhost:8082 | Manage DAGs, view logs |
| Spark Master UI | http://localhost:8080 | Monitor Spark cluster |
| Spark Worker UI | http://localhost:8081 | View worker status |
| Spark Job UI | http://localhost:4040 | View running job details |
| Jupyter Notebook | http://localhost:8888 | Interactive development |
| MinIO Console | http://localhost:9001 | Browse data files |

## Troubleshooting

### DAG Not Appearing

**Problem**: DAG doesn't show up in Airflow UI

**Solutions**:
```bash
# Check scheduler logs
docker-compose logs airflow-scheduler | grep -i error

# Verify DAG syntax
docker-compose exec airflow-scheduler airflow dags list

# Check for Python errors
docker-compose exec airflow-scheduler python /opt/airflow/dags/your_dag.py

# Restart scheduler
docker-compose restart airflow-scheduler
```

### Spark Connection Failing

**Problem**: SparkSubmitOperator fails to connect

**Solutions**:
```bash
# Verify Spark master is running
docker-compose ps spark-master

# Check connection in Airflow
# Go to: Admin > Connections > spark_default

# Test connection manually
docker-compose exec airflow-scheduler spark-submit --version

# Recreate connection
./setup_airflow_connections.sh
```

### Task Failing with S3A Errors

**Problem**: Job fails with S3A or MinIO connection errors

**Solutions**:
- Verify MinIO is running: http://localhost:9001
- Check Spark config includes all S3A settings
- Verify credentials in `conf` parameter
- Check MinIO buckets exist (warehouse, data)

### Permission Issues

**Problem**: Cannot access `/opt/spark-apps` files

**Solutions**:
```bash
# Check volume mounts in docker-compose.yml
docker-compose exec airflow-scheduler ls -la /opt/spark-apps

# Verify file permissions
chmod +r spark-apps/*.py

# Rebuild images
docker-compose build
docker-compose up -d
```

### Memory Issues

**Problem**: Spark job fails with OOM errors

**Solutions**:
```python
# Increase memory in SparkSubmitOperator
SparkSubmitOperator(
    # ...
    driver_memory='4g',
    executor_memory='8g',
)
```

## Best Practices

### 1. Use Standard Spark Configuration

Create a shared config constant:
```python
SPARK_CONFIG = {
    'spark.sql.hive.metastore.version': '3.1.3',
    'spark.sql.catalogImplementation': 'hive',
    'spark.hadoop.hive.metastore.uris': 'thrift://hive-metastore:9083',
    'spark.sql.warehouse.dir': 's3a://warehouse/',
    'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
    'spark.hadoop.fs.s3a.access.key': 'admin',
    'spark.hadoop.fs.s3a.secret.key': 'admin123',
    'spark.hadoop.fs.s3a.path.style.access': 'true',
    'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
}
```

### 2. Add Data Quality Checks

Use `PythonOperator` for validation:
```python
def check_data_quality(**context):
    # Connect to Hive/Spark
    # Verify record counts
    # Check for nulls
    # Validate business rules
    pass

check_quality = PythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
)
```

### 3. Implement Idempotency

Make jobs rerunnable:
```python
# In your Spark jobs, use overwrite mode
df.write.mode("overwrite").insertInto("silver.transactions")

# Or use date partitions
df.write.mode("overwrite").partitionBy("date").insertInto("...")
```

### 4. Use Task Dependencies Wisely

```python
# Parallel processing when possible
bronze >> [silver1, silver2, silver3]

# Sequential when necessary
bronze >> silver >> gold
```

### 5. Enable Retries

```python
default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
```

### 6. Tag Your DAGs

```python
tags=['bronze', 'ingestion', 'production']
```

### 7. Monitor and Alert

- Set `email_on_failure: True`
- Configure SMTP in Airflow
- Use Slack/PagerDuty operators

## Next Steps

1. **Create Silver DAGs**: Build DAGs for bronze → silver transformations
2. **Create Gold DAGs**: Build DAGs for silver → gold aggregations
3. **Schedule Pipelines**: Set appropriate `schedule_interval`
4. **Add Data Quality**: Implement validation tasks
5. **Set Up Alerts**: Configure email/Slack notifications
6. **Monitor Performance**: Track job durations and optimize
7. **Add Documentation**: Document each DAG's purpose and logic

## Additional Resources

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [SparkSubmitOperator Docs](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/)
- [Spark Configuration](https://spark.apache.org/docs/latest/configuration.html)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)

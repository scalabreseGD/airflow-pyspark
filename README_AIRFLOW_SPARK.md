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

### 1. Bronze Data Ingestion DAG

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

### 2. Bronze to Silver Pipeline DAG

**File**: `dags/bronze_to_silver_pipeline_dag.py`

**Purpose**: Orchestrates the complete bronze → silver transformation pipeline

**Pipeline Flow:**
1. **start_pipeline**: Initializes pipeline with logging
2. **trigger_bronze_ingestion**: Triggers the `ingest_bronze_data` DAG
3. **validate_bronze_data**: Validates bronze tables have data
4. **Transform tasks (parallel)**: 6 parallel transformations with concurrency 3
   - transform_transactions_to_silver
   - transform_transaction_items_to_silver
   - transform_subscriptions_to_silver
   - transform_product_catalog_to_silver
   - transform_inventory_snapshots_to_silver
   - transform_customer_interactions_to_silver
5. **complete_pipeline**: Finalizes pipeline with summary

**Concurrency**: 3 parallel jobs (controlled by `concurrency=3`)

**Spark Configuration**: Uses same config as standalone silver jobs:
- 3 cores max per job
- 3 executors with 1 core each
- Full S3A/Hive Metastore integration

**To run manually:**
1. Open Airflow: http://localhost:8082
2. Find `bronze_to_silver_pipeline` DAG
3. Toggle it to "ON" (unpause)
4. Click "Trigger DAG"

### 3. Silver to Gold Pipeline DAG

**File**: `dags/silver_to_gold_pipeline_dag.py`

**Purpose**: Generates business-level analytics from silver layer

**Pipeline Flow:**
1. **start_pipeline**: Initializes pipeline with logging
2. **Gold transformations (parallel)**: 9 parallel analytics jobs with concurrency 3
   - transform_product_performance
   - transform_customer_360
   - transform_store_performance
   - transform_subscription_health
   - transform_basket_analysis
   - transform_campaign_roi_analysis
   - transform_category_brand_performance
   - transform_channel_attribution
   - transform_cohort_analysis
3. **complete_pipeline**: Finalizes pipeline with summary

**Concurrency**: 3 parallel jobs (controlled by `concurrency=3`)

**Spark Configuration**: Uses same config as silver layer for consistency

**Gold Tables Generated:**
- `gold.product_performance` - Product sales and profitability metrics
- `gold.customer_360` - Comprehensive customer profiles
- `gold.store_performance` - Store-level KPIs
- `gold.subscription_health` - Subscription metrics
- `gold.basket_analysis` - Market basket associations
- `gold.campaign_roi_analysis` - Campaign effectiveness
- `gold.category_brand_performance` - Category/brand metrics
- `gold.channel_attribution` - Multi-channel attribution
- `gold.cohort_analysis` - Cohort-based customer analysis

**To run manually:**
1. Open Airflow: http://localhost:8082
2. Find `silver_to_gold_pipeline` DAG
3. Toggle it to "ON" (unpause)
4. Click "Trigger DAG"

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

## Complete Pipeline Architecture

The project now includes a full Bronze → Silver → Gold pipeline:

```
Bronze Layer (Raw Data)
    ↓
[bronze_to_silver_pipeline]
    ↓ (6 parallel jobs, concurrency: 3)
Silver Layer (Cleaned & Validated)
    ↓
[silver_to_gold_pipeline]
    ↓ (9 parallel analytics, concurrency: 3)
Gold Layer (Business Analytics)
```

## Next Steps

1. **Schedule Pipelines**: Set appropriate `schedule_interval` for automated runs
   - Example: Bronze at midnight, Silver at 1 AM, Gold at 3 AM
2. **Chain DAGs**: Use `TriggerDagRunOperator` to create bronze → silver → gold flow
3. **Add Data Quality**: Implement additional validation tasks between layers
4. **Set Up Alerts**: Configure email/Slack notifications for failures
5. **Monitor Performance**: Track job durations and optimize Spark configs
6. **Add Incremental Loading**: Implement date-based incremental processing
7. **Connect BI Tools**: Integrate Tableau/PowerBI to query gold tables

## Additional Resources

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [SparkSubmitOperator Docs](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/)
- [Spark Configuration](https://spark.apache.org/docs/latest/configuration.html)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)

## Automated Data Lineage with Neo4j

All Spark jobs in this project automatically track and push data lineage to Neo4j using the `neo4j_lineage` module.

### Overview

The lineage tracking system captures complete data flows across all Spark jobs:
- **Source datasets**: All tables and files read during job execution
- **Destination datasets**: All tables and files written during job execution
- **Job relationships**: Automatic graph creation showing bronze → silver → gold flows

### How It Works

#### 1. Integration in Spark Jobs

Every Spark job includes the lineage listener:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyJob") \
    .enableHiveSupport() \
    .getOrCreate()

# Enable lineage tracking (automatically tracks all subsequent operations)
try:
    from neo4j_lineage import enable
    enable(spark)
except Exception as e:
    print(f"Warning: Could not enable lineage tracking: {e}")

# All subsequent reads/writes are automatically tracked
spark.sql("USE bronze")
df = spark.table("transactions_raw")  # ✓ Tracked as source

# Transform data...
result_df = df.filter(df.status == "completed")

# Write to silver
result_df.write.mode("overwrite").insertInto("silver.transactions")  # ✓ Tracked as destination
```

#### 2. What Gets Tracked

The listener automatically captures:

**Read Operations:**
- `spark.table("database.table")` - Hive table reads
- `spark.read.parquet("s3a://path")` - File reads (Parquet, CSV, JSON, ORC, text)
- `spark.sql("SELECT * FROM bronze.transactions")` - SQL FROM/JOIN clauses

**Write Operations:**
- `df.write.saveAsTable("silver.table")` - Save as Hive table
- `df.write.insertInto("silver.table")` - Insert into existing table
- `df.write.parquet("s3a://path")` - Write to files
- `spark.sql("INSERT INTO silver.table SELECT ...")` - SQL inserts

#### 3. Graph Structure

The lineage creates a knowledge graph in Neo4j:

**Nodes:**
- `(:Dataset {name: "bronze.transactions_raw"})` - Tables and file paths
- `(:SparkJob {name: "BronzeToSilverTransactions"})` - Spark applications

**Relationships:**
- `(Dataset)-[:FLOWS_TO]->(SparkJob)` - Data read by job
- `(SparkJob)-[:WRITES_TO]->(Dataset)` - Data written by job

**Example Graph:**
```
(s3a://data/source_data/transactions.csv)
    -[:FLOWS_TO]->
(IngestBronzeData)
    -[:WRITES_TO]->
(bronze.transactions_raw)
    -[:FLOWS_TO]->
(BronzeToSilverTransactions)
    -[:WRITES_TO]->
(silver.transactions)
    -[:FLOWS_TO]->
(GoldCustomer360)
    -[:WRITES_TO]->
(gold.customer_360)
```

### Configuration

Control lineage tracking via environment variables (set in `docker-compose.yml`):

```yaml
environment:
  # Enable/disable lineage tracking
  LINEAGE_TO_NEO4J: "true"  # default: true
  
  # Neo4j connection
  NEO4J_URI: "bolt://neo4j:7687"  # default
  NEO4J_USER: "neo4j"  # default
  NEO4J_PASSWORD: "neo4j123"  # default
  
  # Debug logging
  LINEAGE_DEBUG: "true"  # default: true (prints lineage events to logs)
```

To **disable** lineage tracking for a specific job:
```python
import os
os.environ["LINEAGE_TO_NEO4J"] = "false"

# Then create SparkSession...
```

### Setup

#### 1. Start Neo4j

```bash
docker-compose up -d neo4j

# Verify Neo4j is running
docker-compose ps neo4j
```

#### 2. Ensure Spark Images Have Neo4j Driver

The `neo4j` Python package is required for bolt protocol:

```dockerfile
# In docker/spark/Dockerfile (already included)
RUN pip install neo4j
```

Rebuild if needed:
```bash
docker-compose build spark-master spark-worker
docker-compose up -d spark-master spark-worker
```

#### 3. Run Your Spark Jobs

Lineage is automatically tracked when jobs run:

```bash
# Via Airflow (recommended)
# Open http://localhost:8082 and trigger any DAG

# Or direct submission
./submit.sh ingest_bronze_data.py
```

### Viewing Lineage

#### Open Neo4j Browser

Open: **http://localhost:7474** (neo4j / neo4j123)

#### Query Examples

**1. View All Data Flows:**
```cypher
MATCH (src:Dataset)-[:FLOWS_TO]->(job:SparkJob)-[:WRITES_TO]->(dst:Dataset)
RETURN src, job, dst;
```

**2. Trace Upstream Lineage (where does data come from?):**
```cypher
// Find all sources that flow into gold.customer_360
MATCH path = (source:Dataset)-[:FLOWS_TO*1..10]->(job:SparkJob)
      -[:WRITES_TO]->(target:Dataset {name: "gold.customer_360"})
RETURN path;
```

**3. Trace Downstream Lineage (where does data go?):**
```cypher
// Find all tables created from bronze.transactions_raw
MATCH path = (source:Dataset {name: "bronze.transactions_raw"})
      -[:FLOWS_TO]->(job:SparkJob)-[:WRITES_TO*1..10]->(target:Dataset)
RETURN path;
```

**4. Find Jobs Reading a Specific Table:**
```cypher
MATCH (dataset:Dataset {name: "silver.transactions"})-[:FLOWS_TO]->(job:SparkJob)
RETURN dataset, job;
```

**5. Find Jobs Writing to a Specific Table:**
```cypher
MATCH (job:SparkJob)-[:WRITES_TO]->(dataset:Dataset {name: "silver.transactions"})
RETURN job, dataset;
```

**6. Complete Pipeline View (Bronze → Silver → Gold):**
```cypher
MATCH path = (bronze:Dataset)-[:FLOWS_TO]->(j1:SparkJob)-[:WRITES_TO]->
             (silver:Dataset)-[:FLOWS_TO]->(j2:SparkJob)-[:WRITES_TO]->(gold:Dataset)
WHERE bronze.name STARTS WITH "bronze." 
  AND silver.name STARTS WITH "silver."
  AND gold.name STARTS WITH "gold."
RETURN path
LIMIT 20;
```

**7. Impact Analysis (what breaks if I change this table?):**
```cypher
// Find all downstream dependencies
MATCH path = (changed:Dataset {name: "silver.transactions"})
      -[:FLOWS_TO*1..5]->(affected)
RETURN DISTINCT affected.name AS affected_assets
ORDER BY affected_assets;
```

### Use Cases

#### 1. Debugging Pipeline Issues

```cypher
// Find which job writes to a problematic table
MATCH (job:SparkJob)-[:WRITES_TO]->(dataset:Dataset {name: "silver.transactions"})
RETURN job.name, dataset.name;
```

#### 2. Impact Analysis

```cypher
// Before modifying bronze.transactions_raw, see what depends on it
MATCH path = (source:Dataset {name: "bronze.transactions_raw"})
      -[:FLOWS_TO*1..10]->(downstream)
RETURN DISTINCT downstream.name AS affected_tables;
```

#### 3. Data Provenance & Compliance

```cypher
// Document complete lineage for audit
MATCH path = (source)-[:FLOWS_TO*..10]->(target:Dataset {name: "gold.customer_360"})
RETURN path;
```

#### 4. Pipeline Optimization

```cypher
// Find tables read by multiple jobs (candidates for caching)
MATCH (dataset:Dataset)-[:FLOWS_TO]->(job:SparkJob)
WITH dataset, COUNT(DISTINCT job) AS job_count
WHERE job_count > 1
RETURN dataset.name AS frequently_read_table, job_count
ORDER BY job_count DESC;
```

### Troubleshooting

#### Lineage Not Appearing

**Check if lineage is enabled:**
```bash
docker-compose logs spark-worker | grep LINEAGE
# Should see: [LINEAGE] Enabling lineage for app: ...
```

**Verify Neo4j connection:**
```bash
docker-compose ps neo4j

# Test connection
docker exec -it spark-master python3 -c "
from neo4j import GraphDatabase
driver = GraphDatabase.driver('bolt://neo4j:7687', auth=('neo4j', 'neo4j123'))
with driver.session() as s:
    result = s.run('RETURN 1')
    print('Connected:', result.single()[0])
driver.close()
"
```

**Check Spark job logs:**
```bash
docker-compose logs spark-master | grep -A 5 "lineage"
docker-compose logs spark-worker | grep -A 5 "lineage"
```

#### Neo4j Driver Not Found

```bash
# Rebuild Spark images with neo4j package
docker-compose build spark-master spark-worker
docker-compose up -d spark-master spark-worker
```

#### Clear Existing Lineage

```cypher
// Delete all lineage data
MATCH (n) DETACH DELETE n;
```

### Best Practices

1. **Keep Lineage Enabled**: The overhead is minimal and the insights are valuable
2. **Use Meaningful Job Names**: Set descriptive `appName` when creating SparkSession
3. **Query Regularly**: Review lineage after pipeline changes
4. **Document Complex Flows**: Add comments to jobs explaining business logic
5. **Combine with DAG Knowledge Graph**: Use `create-kb/parse-airflow.py` to see both DAG structure and data lineage

### Integration with DAG Knowledge Graph

This lineage tracking complements the DAG knowledge graph created by `create-kb/parse-airflow.py`:

- **DAG Graph**: Shows Airflow task orchestration and Spark job configurations
- **Lineage Graph**: Shows actual data flows between datasets

Together they provide:
- **What runs when**: DAG scheduling and dependencies
- **What reads/writes what**: Data lineage and transformations
- **How it runs**: Resource allocation and configurations

See [create-kb/README.md](create-kb/README.md) for DAG knowledge graph documentation.

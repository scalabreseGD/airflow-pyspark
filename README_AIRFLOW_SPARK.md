# Airflow + PySpark Integration Guide

## Overview

This setup integrates Apache Airflow 2.7.0 with your PySpark cluster using the `SparkSubmitOperator`, which is the proper way to submit Spark jobs from Airflow.

## Architecture

- **Airflow Scheduler**: Runs with custom image including Spark binaries
- **Airflow Webserver**: UI for managing DAGs
- **SparkSubmitOperator**: Submits jobs directly to Spark Master
- **Spark Master**: Receives and schedules Spark jobs
- **LocalExecutor**: Simple, non-scalable executor perfect for local development

## Why SparkSubmitOperator?

Using `SparkSubmitOperator` instead of bash commands provides:

1. **Better Integration**: Native Airflow operator designed for Spark
2. **Connection Management**: Centralized Spark connection configuration
3. **Logging**: Better integration with Airflow's logging system
4. **Error Handling**: Proper task failure detection and retry logic
5. **Monitoring**: Track Spark job status through Airflow UI

## Setup Instructions

### 1. Build and Start Services

```bash
# Build the custom Airflow image with Spark support
docker-compose build airflow-init

# Start all services
docker-compose up -d

# Check Airflow services are running
docker-compose ps | grep airflow
```

### 2. Configure Spark Connection

Run the setup script to create the Spark connection in Airflow:

```bash
./setup_airflow_connections.sh
```

This creates a connection with:
- **Connection ID**: `spark_default`
- **Type**: Spark
- **Host**: `spark://spark-master`
- **Port**: `7077`
- **Deploy Mode**: client

You can also manually create/edit the connection in Airflow UI:
- Go to: http://localhost:8082/connection/list/
- Login: admin / admin
- Add or edit the `spark_default` connection

### 3. Verify Setup

1. **Access Airflow UI**: http://localhost:8082 (admin / admin)
2. **Check DAGs**: You should see `ingest_bronze_data` DAG
3. **Check Connection**: Admin > Connections > `spark_default`

## Using the DAG

### Bronze Data Ingestion DAG

Located at: `dags/ingest_bronze_data_dag.py`

**What it does:**
1. **check_prerequisites**: Validates environment is ready
2. **ingest_bronze_data**: Submits `ingest_bronze_data.py` to Spark cluster
3. **verify_ingestion**: Validates the ingestion completed successfully

**To run manually:**
1. Go to http://localhost:8082
2. Find `ingest_bronze_data` DAG
3. Toggle it to "ON" (unpause)
4. Click the "Play" button and select "Trigger DAG"

**To schedule:**
Edit `dags/ingest_bronze_data_dag.py` and change:
```python
schedule_interval=None  # Manual only
```
To:
```python
schedule_interval='@daily'  # Run daily at midnight
# or
schedule_interval='0 2 * * *'  # Run at 2 AM daily
```

## Creating Your Own DAGs

### Example: Submit Spark Job

```python
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    'my_spark_job',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    submit_job = SparkSubmitOperator(
        task_id='submit_spark_job',
        application='/opt/spark-apps/my_script.py',
        conn_id='spark_default',
        conf={
            'spark.sql.warehouse.dir': 's3a://warehouse/',
            'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
            'spark.hadoop.fs.s3a.access.key': 'admin',
            'spark.hadoop.fs.s3a.secret.key': 'admin123',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
        },
        verbose=True,
    )
```

### SparkSubmitOperator Parameters

- `application`: Path to Python/Scala/Java application (must be accessible from Airflow container)
- `conn_id`: Airflow connection ID for Spark
- `conf`: Dictionary of Spark configuration properties
- `verbose`: Enable verbose logging
- `name`: Spark application name (optional)
- `java_class`: Main class for Scala/Java apps (optional)
- `packages`: Maven packages to include (optional)
- `py_files`: Additional Python files (optional)
- `files`: Additional files to ship (optional)

## Useful Commands

```bash
# View Airflow logs
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver

# List Airflow connections
docker-compose exec airflow-scheduler airflow connections list

# Test a DAG
docker-compose exec airflow-scheduler airflow dags test ingest_bronze_data 2024-01-01

# Rebuild Airflow images (if you modify Dockerfile)
docker-compose build airflow-init
docker-compose up -d airflow-webserver airflow-scheduler

# Access Airflow CLI
docker-compose exec airflow-scheduler airflow <command>
```

## Ports

- **8082**: Airflow Web UI
- **8080**: Spark Master UI
- **8081**: Spark Worker UI
- **9000**: MinIO API
- **9001**: MinIO Console
- **5432**: PostgreSQL
- **9083**: Hive Metastore

## Troubleshooting

### DAG not appearing

- Check scheduler logs: `docker-compose logs airflow-scheduler`
- Verify DAG syntax: `docker-compose exec airflow-scheduler airflow dags list`

### Spark connection failing

- Verify Spark master is running: `docker-compose ps spark-master`
- Check connection: http://localhost:8082/connection/list/
- Test manually: `docker-compose exec airflow-scheduler spark-submit --version`

### Permission issues

- Ensure `/opt/spark-apps` is accessible from Airflow containers
- Check volume mounts in docker-compose.yml

## Next Steps

1. Create additional DAGs for silver/gold layer transformations
2. Add sensors to wait for data availability
3. Implement data quality checks
4. Set up alerting for failed jobs
5. Configure email notifications for task failures
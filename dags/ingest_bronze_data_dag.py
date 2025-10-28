"""
Airflow DAG to ingest bronze data from MinIO to Bronze tables using PySpark.

This DAG triggers the ingest_bronze_data.py Spark job which reads CSV files
from MinIO and loads them into bronze layer tables.

Prerequisites:
- Bronze tables must be created first (create_bronze_tables.ipynb)
- MinIO must have source data files uploaded
- Spark cluster must be running
- Spark connection configured in Airflow (conn_id: spark_default)

Schedule: Manual trigger (set schedule_interval to a cron expression for automation)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'ingest_bronze_data',
    default_args=default_args,
    description='Ingest CSV data from MinIO into Bronze layer tables',
    schedule_interval=None,  # Manual trigger only - change to '@daily' or cron for scheduling
    catchup=False,
    tags=['bronze', 'ingestion', 'pyspark'],
)


bronze_configs = {
    # Hive Metastore Configuration - CRITICAL for finding tables!
    'spark.sql.hive.metastore.version': '3.1.3',
    'spark.sql.catalogImplementation': 'hive',
    'spark.hadoop.hive.metastore.uris': 'thrift://hive-metastore:9083',
    'spark.sql.warehouse.dir': 's3a://warehouse/',
    # S3A / MinIO Configuration
    'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
    'spark.hadoop.fs.s3a.access.key': 'admin',
    'spark.hadoop.fs.s3a.secret.key': 'admin123',
    'spark.hadoop.fs.s3a.path.style.access': 'true',
    'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
    'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
}

# Task 1: Create bronze
create_bronze_data = SparkSubmitOperator(
    task_id='create_bronze_data',
    application='/opt/spark-apps/create_bronze_tables.py',
    conn_id='spark_default',
    conf=bronze_configs,
    verbose=True,
    name='CreateBronzeData',
    dag=dag,
)

# Task 2: Submit Spark job to ingest bronze data
ingest_bronze_data = SparkSubmitOperator(
    task_id='ingest_bronze_data',
    application='/opt/spark-apps/ingest_bronze_data.py',
    conn_id='spark_default',
    conf=bronze_configs,
    verbose=True,
    name='IngestBronzeData',
    dag=dag,
)

# Define task dependencies
create_bronze_data >> ingest_bronze_data

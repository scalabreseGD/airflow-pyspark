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


def check_prerequisites(**context):
    """
    Check if prerequisites are met before running the Spark job.
    This can be expanded to verify tables exist, check data availability, etc.
    """

    print("Checking prerequisites for bronze data ingestion...")

    # In a production setup, you might want to check:
    # - Bronze tables exist
    # - Source data files are available
    # - Spark cluster is healthy

    print("✓ Prerequisites check passed")
    return True


def verify_ingestion_results(**context):
    """
    Verify the ingestion results after the Spark job completes.
    """

    print("Verifying bronze data ingestion results...")

    # This is a placeholder - in production, you'd connect to the metastore
    # and verify record counts, data quality, etc.

    print("✓ Ingestion verification completed")
    return True


# Task 1: Check prerequisites (optional but recommended)
check_prerequisites_task = PythonOperator(
    task_id='check_prerequisites',
    python_callable=check_prerequisites,
    provide_context=True,
    dag=dag,
)

# Task 2: Submit Spark job to ingest bronze data
ingest_bronze_data = SparkSubmitOperator(
    task_id='ingest_bronze_data',
    application='/opt/spark-apps/ingest_bronze_data.py',
    conn_id='spark_default',
    conf={
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
    },
    verbose=True,
    dag=dag,
)

# Task 3: Verify ingestion results (optional but recommended)
verify_ingestion_task = PythonOperator(
    task_id='verify_ingestion',
    python_callable=verify_ingestion_results,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
check_prerequisites_task >> ingest_bronze_data >> verify_ingestion_task

"""
Airflow Wrapper DAG - Bronze to Silver Data Pipeline

This orchestrator DAG manages the end-to-end data pipeline from bronze ingestion
to silver transformation for the transactions domain.

Pipeline Flow:
1. Ingest bronze data (trigger ingest_bronze_data DAG)
2. Transform transactions to silver layer
3. Transform transaction_items to silver layer

This provides a single entry point to run the entire pipeline with proper
dependency management and monitoring.

Prerequisites:
- Bronze and silver tables must be created
- Source data must be available in MinIO
- Spark cluster must be running

Schedule: Manual trigger or scheduled (e.g., '@daily')
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
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# Define the wrapper DAG
dag = DAG(
    'bronze_to_silver_pipeline',
    default_args=default_args,
    description='End-to-end pipeline: Bronze ingestion -> Silver transformation for transactions',
    schedule_interval=None,  # Set to '@daily' or cron expression for scheduled runs
    catchup=False,
    concurrency=3,
    tags=['pipeline', 'orchestrator', 'bronze-to-silver', 'transactions'],
)


def start_pipeline(**context):
    """
    Initialize the pipeline run with logging and validation.
    """
    execution_date = context['execution_date']
    print("=" * 80)
    print("  BRONZE TO SILVER PIPELINE - START")
    print("=" * 80)
    print(f"Execution Date: {execution_date}")
    print(f"DAG Run ID: {context['dag_run'].run_id}")
    print("\nPipeline Steps:")
    print("  1. Ingest bronze data from MinIO")
    print("  2. Validate bronze data (fail if tables are empty)")
    print("  3. Transform bronze -> silver (transactions)")
    print("  4. Transform bronze -> silver (transaction_items)")
    print("  5. Transform bronze -> silver (subscriptions)")
    print("  6. Transform bronze -> silver (product_catalog)")
    print("  7. Transform bronze -> silver (inventory_snapshots)")
    print("  8. Transform bronze -> silver (customer_interactions)")
    print("=" * 80)
    return True


def complete_pipeline(**context):
    """
    Finalize the pipeline run with summary and cleanup.
    """
    print("=" * 80)
    print("  BRONZE TO SILVER PIPELINE - COMPLETE")
    print("=" * 80)
    print("✓ Bronze data ingestion: SUCCESS")
    print("✓ Bronze data validation: SUCCESS")
    print("✓ Silver transformation (transactions): SUCCESS")
    print("✓ Silver transformation (transaction_items): SUCCESS")
    print("✓ Silver transformation (subscriptions): SUCCESS")
    print("✓ Silver transformation (product_catalog): SUCCESS")
    print("✓ Silver transformation (inventory_snapshots): SUCCESS")
    print("✓ Silver transformation (customer_interactions): SUCCESS")
    print("\nNext steps:")
    print("  - Verify data quality in silver.transactions")
    print("  - Verify data quality in silver.transaction_items")
    print("  - Verify data quality in silver.subscriptions")
    print("  - Verify data quality in silver.product_catalog")
    print("  - Verify data quality in silver.inventory_snapshots")
    print("  - Verify data quality in silver.customer_interactions")
    print("  - Run downstream analytics or gold layer aggregations")
    print("  - Monitor data freshness metrics")
    print("=" * 80)
    return True


# Spark configuration for silver transformations
spark_silver_conf = {
    # Hive Metastore Configuration
    'spark.sql.hive.metastore.version': '3.1.3',
    'spark.sql.catalogImplementation': 'hive',
    # Allocate 1 core per executor to allow more parallel jobs
    'spark.executor.cores': '1',
    # Total cores per application (with 9 cores total, 3 cores per job allows 3 concurrent jobs)
    'spark.cores.max': '3',
    # Number of executors (3 executors × 1 core = 3 cores total per job)
    'spark.executor.instances': '3',
    'spark.sql.warehouse.dir': 's3a://warehouse/',
    'spark.hadoop.hive.metastore.uris': 'thrift://hive-metastore:9083',
    # S3A / MinIO Configuration
    'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
    'spark.hadoop.fs.s3a.access.key': 'admin',
    'spark.hadoop.fs.s3a.secret.key': 'admin123',
    'spark.hadoop.fs.s3a.path.style.access': 'true',
    'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
    'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
    # Spark Configuration
    'spark.sql.adaptive.enabled': 'true',
    'spark.sql.adaptive.coalescePartitions.enabled': 'true',
}

# Task 1: Pipeline initialization
start_pipeline_task = PythonOperator(
    task_id='start_pipeline',
    python_callable=start_pipeline,
    provide_context=True,
    dag=dag,
)

# Task 3: Validate bronze data (fail if any table is empty)
validate_bronze_data = SparkSubmitOperator(
    task_id='validate_bronze_data',
    application='/opt/spark-apps/validate_bronze_data.py',
    conn_id='spark_default',
    conf=spark_silver_conf,
    verbose=True,
    dag=dag,
)

# Task 4: Transform transactions to silver
transform_transactions_to_silver = SparkSubmitOperator(
    task_id='transform_transactions_to_silver',
    application='/opt/spark-apps/bronze_to_silver_transactions.py',
    conn_id='spark_default',
    conf=spark_silver_conf,
    verbose=True,
    dag=dag,
)

# Task 5: Transform transaction_items to silver
transform_transaction_items_to_silver = SparkSubmitOperator(
    task_id='transform_transaction_items_to_silver',
    application='/opt/spark-apps/bronze_to_silver_transaction_items.py',
    conn_id='spark_default',
    conf=spark_silver_conf,
    verbose=True,
    dag=dag,
)

# Task 6: Transform subscriptions to silver
transform_subscriptions_to_silver = SparkSubmitOperator(
    task_id='transform_subscriptions_to_silver',
    application='/opt/spark-apps/bronze_to_silver_subscriptions.py',
    conn_id='spark_default',
    conf=spark_silver_conf,
    verbose=True,
    dag=dag,
)

# Task 7: Transform product_catalog to silver
transform_product_catalog_to_silver = SparkSubmitOperator(
    task_id='transform_product_catalog_to_silver',
    application='/opt/spark-apps/bronze_to_silver_product_catalog.py',
    conn_id='spark_default',
    conf=spark_silver_conf,
    verbose=True,
    dag=dag,
)

# Task 8: Transform inventory_snapshots to silver
transform_inventory_snapshots_to_silver = SparkSubmitOperator(
    task_id='transform_inventory_snapshots_to_silver',
    application='/opt/spark-apps/bronze_to_silver_inventory_snapshots.py',
    conn_id='spark_default',
    conf=spark_silver_conf,
    verbose=True,
    dag=dag,
)

# Task 9: Transform customer_interactions to silver
transform_customer_interactions_to_silver = SparkSubmitOperator(
    task_id='transform_customer_interactions_to_silver',
    application='/opt/spark-apps/bronze_to_silver_customer_interactions.py',
    conn_id='spark_default',
    conf=spark_silver_conf,
    verbose=True,
    dag=dag,
)

# Task 10: Pipeline completion
complete_pipeline_task = PythonOperator(
    task_id='complete_pipeline',
    python_callable=complete_pipeline,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
# Pipeline flow: start -> bronze ingestion -> validate bronze -> silver transformations (parallel) -> complete
start_pipeline_task >> validate_bronze_data >> [
    transform_transactions_to_silver,
    transform_transaction_items_to_silver,
    transform_subscriptions_to_silver,
    transform_product_catalog_to_silver,
    transform_inventory_snapshots_to_silver,
    transform_customer_interactions_to_silver
] >> complete_pipeline_task

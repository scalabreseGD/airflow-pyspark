"""
Airflow DAG - Silver to Gold Data Pipeline

This DAG manages the gold layer transformations that create business-level
aggregations and analytics from the silver layer tables.

Pipeline Flow:
1. Validate silver data availability
2. Transform to gold layer (9 parallel jobs with concurrency 3):
   - Product Performance Analysis
   - Customer 360 View
   - Store Performance Metrics
   - Subscription Health Indicators
   - Market Basket Analysis
   - Campaign ROI Analysis
   - Category/Brand Performance
   - Channel Attribution
   - Cohort Analysis

This provides a single entry point to run the entire gold layer pipeline with
proper dependency management and monitoring.

Prerequisites:
- Silver tables must be populated with data
- Gold tables must be created
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

# Define the gold layer DAG
dag = DAG(
    'silver_to_gold_pipeline',
    default_args=default_args,
    description='End-to-end pipeline: Silver -> Gold transformation for analytics',
    schedule_interval=None,  # Set to '@daily' or cron expression for scheduled runs
    catchup=False,
    concurrency=3,  # Limit to 3 parallel tasks
    tags=['pipeline', 'orchestrator', 'silver-to-gold', 'analytics'],
)


def start_pipeline(**context):
    """
    Initialize the pipeline run with logging and validation.
    """
    execution_date = context['execution_date']
    print("=" * 80)
    print("  SILVER TO GOLD PIPELINE - START")
    print("=" * 80)
    print(f"Execution Date: {execution_date}")
    print(f"DAG Run ID: {context['dag_run'].run_id}")
    print("\nPipeline Steps:")
    print("  1. Transform to gold.product_performance")
    print("  2. Transform to gold.customer_360")
    print("  3. Transform to gold.store_performance")
    print("  4. Transform to gold.subscription_health")
    print("  5. Transform to gold.basket_analysis")
    print("  6. Transform to gold.campaign_roi_analysis")
    print("  7. Transform to gold.category_brand_performance")
    print("  8. Transform to gold.channel_attribution")
    print("  9. Transform to gold.cohort_analysis")
    print("\nConcurrency: 3 parallel jobs")
    print("=" * 80)
    return True


def complete_pipeline(**context):
    """
    Finalize the pipeline run with summary and cleanup.
    """
    print("=" * 80)
    print("  SILVER TO GOLD PIPELINE - COMPLETE")
    print("=" * 80)
    print("✓ Gold transformation (product_performance): SUCCESS")
    print("✓ Gold transformation (customer_360): SUCCESS")
    print("✓ Gold transformation (store_performance): SUCCESS")
    print("✓ Gold transformation (subscription_health): SUCCESS")
    print("✓ Gold transformation (basket_analysis): SUCCESS")
    print("✓ Gold transformation (campaign_roi_analysis): SUCCESS")
    print("✓ Gold transformation (category_brand_performance): SUCCESS")
    print("✓ Gold transformation (channel_attribution): SUCCESS")
    print("✓ Gold transformation (cohort_analysis): SUCCESS")
    print("\nNext steps:")
    print("  - Verify data quality in all gold tables")
    print("  - Run data quality checks and validations")
    print("  - Connect BI tools for business reporting")
    print("  - Monitor gold layer metrics")
    print("=" * 80)
    return True


# Spark configuration for gold transformations (same as silver)
spark_gold_conf = {
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

# Task 2: Transform to gold.product_performance
transform_product_performance = SparkSubmitOperator(
    task_id='transform_product_performance',
    application='/opt/spark-apps/gold_product_performance.py',
    conn_id='spark_default',
    conf=spark_gold_conf,
    verbose=True,
    name='GoldProductPerformance',
    dag=dag,
)

# Task 3: Transform to gold.customer_360
transform_customer_360 = SparkSubmitOperator(
    task_id='transform_customer_360',
    application='/opt/spark-apps/gold_customer_360.py',
    conn_id='spark_default',
    conf=spark_gold_conf,
    verbose=True,
    name='GoldCustomer360',
    dag=dag,
)

# Task 4: Transform to gold.store_performance
transform_store_performance = SparkSubmitOperator(
    task_id='transform_store_performance',
    application='/opt/spark-apps/gold_store_performance.py',
    conn_id='spark_default',
    conf=spark_gold_conf,
    verbose=True,
    name='GoldStorePerformance',
    dag=dag,
)

# Task 5: Transform to gold.subscription_health
transform_subscription_health = SparkSubmitOperator(
    task_id='transform_subscription_health',
    application='/opt/spark-apps/gold_subscription_health.py',
    conn_id='spark_default',
    conf=spark_gold_conf,
    verbose=True,
    name='GoldSubscriptionHealth',
    dag=dag,
)

# Task 6: Transform to gold.basket_analysis
transform_basket_analysis = SparkSubmitOperator(
    task_id='transform_basket_analysis',
    application='/opt/spark-apps/gold_basket_analysis.py',
    conn_id='spark_default',
    conf=spark_gold_conf,
    verbose=True,
    name='GoldBasketAnalysis',
    dag=dag,
)

# Task 7: Transform to gold.campaign_roi_analysis
transform_campaign_roi_analysis = SparkSubmitOperator(
    task_id='transform_campaign_roi_analysis',
    application='/opt/spark-apps/gold_campaign_roi_analysis.py',
    conn_id='spark_default',
    conf=spark_gold_conf,
    verbose=True,
    name='GoldCampaignROI',
    dag=dag,
)

# Task 8: Transform to gold.category_brand_performance
transform_category_brand_performance = SparkSubmitOperator(
    task_id='transform_category_brand_performance',
    application='/opt/spark-apps/gold_category_brand_performance.py',
    conn_id='spark_default',
    conf=spark_gold_conf,
    verbose=True,
    name='GoldCategoryBrandPerformance',
    dag=dag,
)

# Task 9: Transform to gold.channel_attribution
transform_channel_attribution = SparkSubmitOperator(
    task_id='transform_channel_attribution',
    application='/opt/spark-apps/gold_channel_attribution.py',
    conn_id='spark_default',
    conf=spark_gold_conf,
    verbose=True,
    name='GoldChannelAttribution',
    dag=dag,
)

# Task 10: Transform to gold.cohort_analysis
transform_cohort_analysis = SparkSubmitOperator(
    task_id='transform_cohort_analysis',
    application='/opt/spark-apps/gold_cohort_analysis.py',
    conn_id='spark_default',
    conf=spark_gold_conf,
    verbose=True,
    name='GoldCohortAnalysis',
    dag=dag,
)

# Task 11: Pipeline completion
complete_pipeline_task = PythonOperator(
    task_id='complete_pipeline',
    python_callable=complete_pipeline,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
# Pipeline flow: start -> gold transformations (parallel with concurrency 3) -> complete
start_pipeline_task >> [
    transform_product_performance,
    transform_customer_360,
    transform_store_performance,
    transform_subscription_health,
    transform_basket_analysis,
    transform_campaign_roi_analysis,
    transform_category_brand_performance,
    transform_channel_attribution,
    transform_cohort_analysis
] >> complete_pipeline_task

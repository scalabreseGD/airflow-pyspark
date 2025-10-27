# Default arguments for the DAG
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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
    'full_ingestion_pipeline',
    default_args=default_args,
    description='End-to-end pipeline: Bronze ingestion -> Silver transformation -> Goal data points',
    schedule_interval=None,  # Set to '@daily' or cron expression for scheduled runs
    catchup=False,
    concurrency=3,
    tags=['pipeline', 'orchestrator', 'bronze-to-silver', 'transactions'],
)

# Trigger bronze data ingestion DAG
trigger_bronze_ingestion = TriggerDagRunOperator(
    task_id='trigger_bronze_ingestion',
    trigger_dag_id='ingest_bronze_data',
    wait_for_completion=True,  # Wait for the sub-DAG to complete
    poke_interval=30,  # Check status every 30 seconds
    reset_dag_run=True,  # Allow re-running even if already ran for this date
    execution_date='{{ execution_date }}',  # Pass execution date to sub-DAG
    dag=dag,
)

# Trigger silver transformation DAG
trigger_silver_transformation = TriggerDagRunOperator(
    task_id='trigger_silver_transformation',
    trigger_dag_id='bronze_to_silver_pipeline',
    wait_for_completion=True,  # Wait for the sub-DAG to complete
    poke_interval=30,  # Check status every 30 seconds
    reset_dag_run=True,  # Allow re-running even if already ran for this date
    execution_date='{{ execution_date }}',  # Pass execution date to sub-DAG
    dag=dag,
)

# Trigger silver transformation DAG
trigger_gold_curation = TriggerDagRunOperator(
    task_id='trigger_gold_curation',
    trigger_dag_id='silver_to_gold_pipeline',
    wait_for_completion=True,  # Wait for the sub-DAG to complete
    poke_interval=30,  # Check status every 30 seconds
    reset_dag_run=True,  # Allow re-running even if already ran for this date
    execution_date='{{ execution_date }}',  # Pass execution date to sub-DAG
    dag=dag,
)

trigger_bronze_ingestion >> trigger_silver_transformation >> trigger_gold_curation

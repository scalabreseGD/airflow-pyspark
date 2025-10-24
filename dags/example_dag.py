"""
Example Airflow DAG to demonstrate basic functionality.
This is a simple DAG that can be extended to orchestrate your Spark jobs.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'example_dag',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['example'],
)

def print_hello():
    """Simple Python function to print hello"""
    print("Hello from Airflow!")
    return "Task completed successfully"

# Task 1: Print date
task_print_date = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

# Task 2: Python function
task_hello = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

# Task 3: Check Spark apps directory
task_check_spark = BashOperator(
    task_id='check_spark_apps',
    bash_command='ls -la /opt/spark-apps || echo "Spark apps directory not found"',
    dag=dag,
)

# Define task dependencies
task_print_date >> task_hello >> task_check_spark
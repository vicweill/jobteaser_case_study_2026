from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator # Pour lancer dbt
from datetime import datetime, timedelta
import requests
import pandas as pd

# 1. Default arguments
default_args = {
    'owner': 'analytics_team',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. Logic for Task 1: Fetching
def fetch_raw_data():
    print("Fetching data from the source...")
    return {"data": [1, 2, 3]}

# 3. Logic for Task 2: Processing
def process_data():
    print("Processing and cleaning the data...")

# 4. DAG Definition
with DAG(
    dag_id='career_explorer_sync',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Task A: Fetch
    step_fetch = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_raw_data,
    )

    # Task B: Process
    step_process = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
    )

    # 5. Defining Dependencies
    # This means 'step_process' will only run after 'step_fetch' succeeds.
    step_fetch >> step_process
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os

# Custom path setup
AIRFLOW_HOME = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(AIRFLOW_HOME))
from scripts.fetch_openedx import fetch_block_names

default_args = {
    'owner': 'analytics',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='career_explorer_sync',
    default_args=default_args,
    description='Full pipeline, from openEDX to dbt transformations',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    def run_data_enrichment():
        # Read env variables, with default names here
        api_token = os.getenv("OPENEDX_API_TOKEN", "DEMO")
        input_csv = os.getenv("INPUT_CSV_PATH", "data/existing_blocks.csv")
        output_csv = os.getenv("OUTPUT_CSV_PATH", "data/ref_block_names.csv")

        root_dir = os.path.dirname(AIRFLOW_HOME)
        abs_input = os.path.join(root_dir, input_csv)
        abs_output = os.path.join(root_dir, output_csv)

        # Use function that calls OpenEDX API
        fetch_block_names(
            input_path=abs_input,
            output_path=abs_output,
            api_token=api_token
        )

    task_api_enrichment = PythonOperator(
        task_id='fetch_openedx_api',
        python_callable=run_data_enrichment,
    )

    # Run dbt transformation with models that were previously done
    dbt_project_path = os.path.join(os.path.dirname(AIRFLOW_HOME), "dbt_jobteaser")

    task_dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {dbt_project_path} && uv run dbt run',
    )

    # TASK 3 : Tests de qualité dbt
    task_dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {dbt_project_path} && uv run dbt test',
    )

    task_api_enrichment >> task_dbt_run >> task_dbt_test

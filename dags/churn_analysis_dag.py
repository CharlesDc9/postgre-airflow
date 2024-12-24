import os
import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator


dag_path = os.path.dirname(os.path.abspath(__file__))
project_path = os.path.dirname(dag_path)
sys.path.append(project_path)

from scripts.write_csv_to_postgres import write_csv_to_postgres_main
from scripts.create_modify_df import main as create_modify_df_main

# DAG configuration
default_args = {
    'owner': 'charlesdecian',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

# Create DAG
with DAG(
    'churn_analysis_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Task 1: Load initial data
    load_initial_data = PythonOperator(
        task_id='load_initial_data',
        python_callable=write_csv_to_postgres_main,
        retries=1,
        retry_delay=timedelta(seconds=15)
    )

    # Task 2: Create analysis tables
    create_analysis_tables = PythonOperator(
        task_id='create_analysis_tables',
        python_callable=create_modify_df_main,
        retries=1,
        retry_delay=timedelta(seconds=15)
    )

    # Set task dependencies
    load_initial_data >> create_analysis_tables
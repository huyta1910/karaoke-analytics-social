"""
Airflow DAG for Social Analytics Pipeline
Runs daily: Ingestion (Facebook/GA4) -> dbt Transformation
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Default arguments
default_args = {
    'owner': 'analytics',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'social_analytics_pipeline',
    default_args=default_args,
    description='Ingest Facebook/GA4 data and run dbt transformations',
    schedule_interval='0 6 * * *',  
    start_date=days_ago(1),
    catchup=False,
    tags=['social', 'analytics', 'dbt'],
)

# Paths - adjust these for your environment
PROJECT_DIR = '/opt/airflow/dags/karaoke-analytics-social'
DBT_PROJECT_DIR = f'{PROJECT_DIR}/dbt_social_analytics'
EL_PIPELINE_DIR = f'{PROJECT_DIR}/el_pipeline'

# Task 1: Run Facebook/GA4 Ingestion
run_ingestion = BashOperator(
    task_id='run_ingestion',
    bash_command=f'cd {EL_PIPELINE_DIR} && python main.py',
    dag=dag,
)

# Task 2: Run dbt models
run_dbt = BashOperator(
    task_id='run_dbt_models',
    bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --profiles-dir .',
    dag=dag,
)

# Task 3: Run dbt tests
run_dbt_tests = BashOperator(
    task_id='run_dbt_tests',
    bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --profiles-dir .',
    dag=dag,
)

# Task 4: Generate dbt docs (optional)
generate_docs = BashOperator(
    task_id='generate_dbt_docs',
    bash_command=f'cd {DBT_PROJECT_DIR} && dbt docs generate --profiles-dir .',
    dag=dag,
)

# Define task dependencies
run_ingestion >> run_dbt >> run_dbt_tests >> generate_docs

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 5,
    'retry_delay': timedelta(minutes=15),
    'depends_on_past': False
}

dag = DAG(
    'daily_collector',
    default_args=default_args,
    description='Runs collector.py daily',
    schedule_interval='@daily',
    catchup=False
)

run_collector = BashOperator(
    task_id='run_collector',
    bash_command='python /scripts/collector.py',
    dag=dag
)
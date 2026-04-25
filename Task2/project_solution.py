from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Визнач шлях до своєї папки проекту
PROJECT_PATH = "/home/marichka/datalake_project"

default_args = {
    'owner': 'marichka',
    'start_date': datetime(2025, 4, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'marichka_datalake_automation',
    default_args=default_args,
    schedule_interval=None,  # Запускаємо вручну
    catchup=False,
    tags=['marichka', 'datalake']
) as dag:

    # 1. Landing to Bronze
    task_landing_to_bronze = BashOperator(
        task_id='landing_to_bronze',
        bash_command=f'python {PROJECT_PATH}/landing_to_bronze.py'
    )

    # 2. Bronze to Silver
    task_bronze_to_silver = BashOperator(
        task_id='bronze_to_silver',
        bash_command=f'python {PROJECT_PATH}/bronze_to_silver.py'
    )

    # 3. Silver to Gold
    task_silver_to_gold = BashOperator(
        task_id='silver_to_gold',
        bash_command=f'python {PROJECT_PATH}/silver_to_gold.py'
    )

    # Послідовність виконання
    task_landing_to_bronze >> task_bronze_to_silver >> task_silver_to_gold
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import (
    datetime, 
    timedelta
)

def fetch_and_print_vars():
    import os
    for key, value in os.environ.items():
        print(f"{key}: {value}")

with DAG(
    dag_id="fetch_env_vars_os",
    start_date=datetime.now(),
    schedule_interval=None,
) as dag:

    fetch_vars = PythonOperator(
        task_id="fetch_vars",
        python_callable=fetch_and_print_vars,
    )

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import (
    datetime, 
    timedelta
)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 7, 30, 23, 59),
    'backfill': False,
    'retry_delay': timedelta(minutes=5)
}

with DAG("load-sales-dm-file-data-to-bq-daily-v1.0",default_args=default_args,schedule_interval="30 10 * * *",max_active_runs=1,catchup=False) as dag:
        

    dataproc_cluster_running = DummyOperator(
        task_id='dataproc_cluster_running'
    )

    dataproc_cluster_running
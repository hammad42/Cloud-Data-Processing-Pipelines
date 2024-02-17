from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.hooks.dataproc import DataprocHook#
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitPySparkJobOperator,
    DataprocDeleteClusterOperator,
    ClusterGenerator
)

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

with DAG("load-sales-dm-file-data-to-bq-daily-v1.0",default_args=default_args,schedule_interval=None,max_active_runs=1,catchup=False) as dag:
        

    dataproc_cluster_running = DummyOperator(
        task_id='dataproc_cluster_running'
    )
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        cluster_name='dataproc-cluster',  # Dynamically named cluster
        num_workers=2,
        master_machine_type='n2-standard-2',
        worker_machine_type='n2-standard-2',
        region='us-central1',  # Replace with your desired region
        project_id='playground-s-11-e3c70ff1',
    )

    dataproc_cluster_running >> create_cluster

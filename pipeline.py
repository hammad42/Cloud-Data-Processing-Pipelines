from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.hooks.dataproc import DataprocHook#
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
    ClusterGenerator
)
import pandas as pd

from datetime import (
    datetime, 
    timedelta
)
# from google.cloud import dataproc_v1

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 7, 30, 23, 59),
    'backfill': False,
    'retry_delay': timedelta(minutes=5)
}

with DAG("ETL_Pipeline",default_args=default_args,schedule_interval=None,max_active_runs=1,catchup=False) as dag:
        

    dataproc_cluster_running = DummyOperator(
        task_id='dataproc_cluster_running'
    )
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        cluster_name='dataproc-cluster2',  # Dynamically named cluster
        num_workers=2,
        # enable_component_gateway=True,
        master_machine_type='n2-standard-2',
        worker_machine_type='n2-standard-2',
        region='us-central1',  # Replace with your desired region
        # project_id='playground-s-11-5c4091ae',
        image_version="2.1-debian11"
    )

    job_={
        "placement":{"cluster_name": 'dataproc-cluster2'},
        "pyspark_job":{"main_python_file_uri":"gs://data-bucket522/files/main.py"}


    }
    submit_job = DataprocSubmitJobOperator(
        # project_id='playground-s-11-5c4091ae',
        region='us-central1',
        job=job_,
        task_id='submit_spark_job',
        # job_name='pysparkJob',  # Dynamically named job
        # cluster_name='dataproc-cluster2',  # Reference the created cluster

    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        cluster_name='dataproc-cluster2',  # Dynamically named cluster
        region='us-central1',  # Replace with your desired region
        # project_id='playground-s-11-5c4091ae',
    )

    dataproc_cluster_running >> create_cluster >> submit_job >> delete_cluster

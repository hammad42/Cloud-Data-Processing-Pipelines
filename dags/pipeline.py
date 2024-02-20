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
import os
# from google.cloud import dataproc_v1

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 7, 30, 23, 59),
    'backfill': False,
    'retry_delay': timedelta(minutes=5)
}
project_id_=os.environ.get("GCP_PROJECT")
pyspark_file=os.environ.get("pyspark_file")

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
        project_id=project_id_,
        image_version="2.1-debian11"
    )

    job_={
        "placement":{"cluster_name": 'dataproc-cluster2'},
        "pyspark_job":{"main_python_file_uri":pyspark_file}


    }
    submit_job = DataprocSubmitJobOperator(
        project_id=project_id_,
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
        project_id=project_id_,
    )

    dataproc_cluster_running >> create_cluster >> submit_job >> delete_cluster

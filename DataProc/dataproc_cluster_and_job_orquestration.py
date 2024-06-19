"""
Author: Alan  Ignacio Sepúlveda Rodriguez
Mail: alan.ignacio300@gmail.com
Date: 2021-07-01
Description: This module is in charge of orquestrating a Google Cloud DataProc data processing pipeline using Apache Airflow.
"""

# Importing libraries
import os
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator
)
from datetime import datetime

# GCP Params
PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'project-id')
BUCKET_NAME = os.environ.get('GCP_BUCKET_NAME', 'gcs-bucket-name')
CLUSTER_NAME = os.environ.get('GCP_CLUSTER_NAME', 'composer-cluster-name')
REGION = os.environ.get('GCP_REGION', 'composer-cluster-region')
ZONE = os.environ.get('GCP_ZONE', 'composer-cluster-zone')


# Cluster config DataProcCreateClusterOperator
INIT_ACTIONS = [
    'gs://dataproc-initialization-actions/connectors/connectors.sh']

# Cluster config
CLUSTER_CONFIG = ClusterGenerator(project_id=PROJECT_ID,
                                  num_workers=2,
                                  zone=ZONE,
                                  master_machine_type='n1-standard-2',
                                  worker_machine_type='n1-standard-2',
                                  storage_bucket=BUCKET_NAME,
                                  init_actions_uris=INIT_ACTIONS,
                                  metadata={'bigquery-connector-version':"1.2.0","spark-bigquery-connector-version":"0.36.3"}).make()

# Pyspark  Job configuration
PYSPARK_SCRIPT_URI = f'gs://{BUCKET_NAME}/jobs/pyspark-bq-to-gcs.py'

PYSPARK_JOB = {
    'reference': {'project_id': PROJECT_ID},
    'placement': {
        'cluster_name': CLUSTER_NAME
    },
    'pyspark_job': {
        'main_python_file_uri': PYSPARK_SCRIPT_URI
    }
}

# Defining the DAG
with (models.DAG(
        'dataproc__cluster_and_job_orquestration_demo',
        schedule_interval='@once',
        start_date=datetime(2024,6,18),
        catchup=False,
        tags=["dataproc", "demo"],
        description='Orquestrating a DataProc pipeline')
as dag):

    # Creating the cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_cluster',
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME
    )

    # Submitting the job
    submit_job = DataprocSubmitJobOperator(
        task_id='submit_job',
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID
    )

    # Deleting the cluster
    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_cluster',
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule='all_done'
    )

    # Defining the order of the tasks
    create_cluster >> submit_job >> delete_cluster



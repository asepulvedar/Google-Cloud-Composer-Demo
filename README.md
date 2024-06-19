# Google Cloud Composer Demo

This repository contains demo scripts for orchestrating Google Cloud services using Google Cloud Composer. The demos primarily focus on using Dataproc for running PySpark jobs.

## Repository Structure

- **Dataproc/**: Directory containing scripts related to Google Cloud Dataproc.
  - **pysparkjob/**: Subdirectory containing PySpark job scripts.
    - **pyspark-bq-to-gcs.py**: PySpark script to read data from BigQuery and write to Google Cloud Storage.
  - **dataproc_cluster_and_job_orquestration.py**: Script to orchestrate the creation of a Dataproc cluster and submission of a PySpark job.

## Files

### Dataproc

#### pysparkjob

- **pyspark-bq-to-gcs.py**

  This script performs the following tasks:
  - Reads data from a specified BigQuery table.
  - Processes the data using PySpark.
  - Writes the processed data to a specified Google Cloud Storage bucket.

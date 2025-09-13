from __future__ import annotations

import logging
from datetime import datetime, timedelta

import boto3
from airflow.decorators import task, dag
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# Set up logging for the DAG
log = logging.getLogger(__name__)

# Define S3 bucket and paths
bucket_name = "my-allproject-logs"
LOG_S3_PATH = "logs/financial_risk_processing.log"
local_log_file_path = "/opt/airflow/logs/financial_risk_pipeline/run_pyspark_script/latest/0.log"

# Function to upload logs to S3
@task
def upload_logs_to_s3(local_log_file_path: str, s3_path: str, bucket_name: str):
    """
    Uploads a specified local log file to an S3 bucket.
    """
    log.info(f"Uploading log file from {local_log_file_path} to s3://{bucket_name}/{LOG_S3_PATH}")
    try:
        s3_client = boto3.client('s3')
        s3_client.upload_file(local_log_file_path, bucket_name, LOG_S3_PATH)
        log.info("Log file uploaded successfully.")
    except Exception as e:
        log.error(f"Failed to upload log file: {e}")
        raise

# Define default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize DAG using the TaskFlow API
@dag(
    dag_id='financial_risk_pipeline',
    default_args=default_args,
    description='ETL Pipeline for Financial Risk Data',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['pyspark', 's3', 'etl'],
)

def financial_risk_pipeline():
    """
    A financial risk data processing pipeline.
    """
    # Task 1: Run PySpark script
    # The --master local[*] is for a local Spark session on the same machine.
    run_pyspark_task = BashOperator(
        task_id='run_pyspark_script',
        bash_command='spark-submit/path/to/financial_risk.py',
    )

    # Task 2: Upload Logs to S3
    # Use the TaskFlow API to call the upload function with parameters
    upload_logs_to_s3(
        local_log_file_path=LOCAL_LOG_FILE,
        s3_path=LOG_S3_PATH,
        bucket_name=BUCKET_NAME
    )

    # Define task dependencies
    run_pyspark_task >> upload_logs_to_s3


# Instantiate the DAG
financial_risk_pipeline()


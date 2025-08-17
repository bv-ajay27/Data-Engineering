from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import boto3

# Define S3 bucket and paths
BUCKET_NAME = "my-allproject-logs"
LOG_FILE_PATH = "financial_risk_processing.log"
LOG_S3_PATH = f"logs/{LOG_FILE_PATH}"

# Function to upload logs to S3
def upload_logs_to_s3():
    s3_client = boto3.client('s3')
    s3_client.upload_file(LOG_FILE_PATH, BUCKET_NAME, LOG_S3_PATH)

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

# Initialize DAG
dag = DAG(
    'financial_risk_pipeline',
    default_args=default_args,
    description='ETL Pipeline for Financial Risk Data',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Task 1: Run PySpark script
run_pyspark_task = BashOperator(
    task_id='run_pyspark_script',
    bash_command='spark-submit --deploy-mode cluster /path/to/pyspark_script.py',
    dag=dag,
)

# Task 2: Upload Logs to S3
upload_logs_task = PythonOperator(
    task_id='upload_logs_to_s3',
    python_callable=upload_logs_to_s3,
    dag=dag,
)

# Task 3: Send success email
send_success_email = EmailOperator(
    task_id='send_success_email',
    to='your_email@example.com',
    subject='Financial Risk Pipeline - SUCCESS',
    html_content='<p>The Financial Risk ETL pipeline has completed successfully.</p>',
    dag=dag,
)

# Task 4: Send failure email
send_failure_email = EmailOperator(
    task_id='send_failure_email',
    to='your_email@example.com',
    subject='Financial Risk Pipeline - FAILURE',
    html_content='<p>The Financial Risk ETL pipeline has failed. Please check logs.</p>',
    trigger_rule='one_failed',
    dag=dag,
)

# Define task dependencies
run_pyspark_task >> upload_logs_task >> send_success_email
run_pyspark_task >> send_failure_email


# AIRFLOW DAG TO RUN THE SPARK JOB FOR RETAIL BEHAVIORAL ANALYSIS

# Import required libraries
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 12), # Adjusted start date
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='retail_behavioral_analysis_pipeline',
    default_args=default_args,
    description='A DAG to run Spark job for Customer Segmentation (RFM) and Market Basket Analysis (MBA).',
    schedule_interval='@daily', # Run once every day
    catchup=False,
    tags=['retail', 'spark', 'behavioral-analysis'],
) as dag:

    # Define the Spark Submit task in local mode
    # This task will run Spark job locally without a cluster
    submit_spark_job = SparkSubmitOperator(
        task_id='submit_retail_analytics_spark_job',
        application='/opt/bitnami/spark/jobs/transform_job.py', 
        verbose=False,
        # Use local mode configuration
        conf={'spark.master': 'local[*]'},
        # Use newer PostgreSQL driver version
        packages='org.postgresql:postgresql:42.7.3'
    )
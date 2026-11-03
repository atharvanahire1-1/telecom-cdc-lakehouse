import os
import boto3
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def upload_to_aws_s3():
    """Uploads telecom CDC data to AWS S3 using environment credentials."""
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    bucket_name = os.getenv('AWS_S3_BUCKET')
    local_file_path = '/opt/airflow/stream_landing/telecom_cdc_batch.csv'
    s3_key = 'raw/telecom_cdc_batch.csv'

    if not os.path.exists(local_file_path):
        with open(local_file_path, 'w') as f:
            f.write("customer_id,churn_status,timestamp\n1001,active,2026-11-02T12:00:00")

    s3_client.upload_file(local_file_path, bucket_name, s3_key)
    print(f"Successfully uploaded {local_file_path} to s3://{bucket_name}/{s3_key}")


# 1. Bronze Load: Ingest raw file from S3 into Snowflake landing table
COPY_S3_TO_SNOWFLAKE_SQL = f"""
USE SCHEMA PUBLIC;
COPY INTO telecom_cdc_raw (customer_id, churn_status, timestamp)
FROM 's3://{os.getenv("AWS_S3_BUCKET")}/raw/telecom_cdc_batch.csv'
CREDENTIALS = (
    AWS_KEY_ID = '{os.getenv("AWS_ACCESS_KEY_ID")}'
    AWS_SECRET_KEY = '{os.getenv("AWS_SECRET_ACCESS_KEY")}'
)
FILE_FORMAT = (
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
)
ON_ERROR = 'CONTINUE';
"""

# 2. Silver Transformation: Deduplicate, parse JSON, and calculate deltas
SILVER_TRANSFORMATION_SQL = """
USE SCHEMA SILVER;

CREATE OR REPLACE TABLE silver_cdc_events (
    change_id INTEGER,
    customer_id INTEGER,
    operation VARCHAR(20),
    change_timestamp TIMESTAMP_NTZ,
    old_status VARCHAR(20),
    new_status VARCHAR(20),
    old_data_used FLOAT,
    new_data_used FLOAT,
    data_usage_delta FLOAT,
    is_churn_event BOOLEAN,
    source_file_name STRING,
    file_row_number NUMBER,
    silver_processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO silver_cdc_events (
    change_id,
    customer_id,
    operation,
    change_timestamp,
    old_status,
    new_status,
    old_data_used,
    new_data_used,
    data_usage_delta,
    is_churn_event,
    source_file_name,
    file_row_number
)
SELECT 
    raw_payload:change_id::INTEGER AS change_id,
    raw_payload:customer_id::INTEGER AS customer_id,
    raw_payload:operation::VARCHAR AS operation,
    raw_payload:timestamp::TIMESTAMP_NTZ AS change_timestamp,
    raw_payload:before:status::VARCHAR AS old_status,
    raw_payload:after:status::VARCHAR AS new_status,
    raw_payload:before:data_used::FLOAT AS old_data_used,
    raw_payload:after:data_used::FLOAT AS new_data_used,
    ROUND(raw_payload:after:data_used::FLOAT - raw_payload:before:data_used::FLOAT, 2) AS data_usage_delta,
    (raw_payload:before:status::VARCHAR = 'active' AND raw_payload:after:status::VARCHAR = 'churned') AS is_churn_event,
    source_file_name,
    file_row_number
FROM TELECOM_LAKEHOUSE.BRONZE.raw_cdc_events
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY raw_payload:change_id::INTEGER 
    ORDER BY raw_payload:timestamp::TIMESTAMP_NTZ DESC, ingestion_timestamp DESC
) = 1;
"""

# 3. Gold Aggregation: Build analytics-ready churn metrics
GOLD_AGGREGATION_SQL = """
USE SCHEMA GOLD;

CREATE OR REPLACE TABLE fact_churn_summary AS
SELECT 
    CASE 
        WHEN new_data_used < 1000 THEN 'Low Usage (< 1,000 MB)'
        WHEN new_data_used BETWEEN 1000 AND 5000 THEN 'Medium Usage (1,000 - 5,000 MB)'
        ELSE 'High Usage (5,000 - 10,000 MB)'
    END AS usage_segment,
    COUNT(*) AS total_churn_events,
    ROUND(AVG(new_data_used), 2) AS avg_final_data_used,
    ROUND(SUM(new_data_used) / 1024, 2) AS total_data_consumed_gb,
    CURRENT_TIMESTAMP() AS gold_processed_at
FROM TELECOM_LAKEHOUSE.SILVER.silver_cdc_events
WHERE is_churn_event = TRUE
GROUP BY 1;
"""

with DAG(
    'telecom_lakehouse_pipeline',
    default_args=default_args,
    description='End-to-End Telecom CDC Lakehouse Pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['telecom', 'aws', 'snowflake', 'medallion'],
) as dag:

    start_pipeline = EmptyOperator(task_id='start_pipeline')

    s3_upload_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_aws_s3
    )

    load_s3_to_snowflake = SnowflakeOperator(
        task_id='load_bronze_landing',
        snowflake_conn_id='snowflake_default',
        sql=COPY_S3_TO_SNOWFLAKE_SQL
    )

    transform_silver = SnowflakeOperator(
        task_id='transform_silver_layer',
        snowflake_conn_id='snowflake_default',
        sql=SILVER_TRANSFORMATION_SQL
    )

    aggregate_gold = SnowflakeOperator(
        task_id='aggregate_gold_layer',
        snowflake_conn_id='snowflake_default',
        sql=GOLD_AGGREGATION_SQL
    )

    end_pipeline = EmptyOperator(task_id='end_pipeline')

    # End-to-end dependency chain
    (
        start_pipeline 
        >> s3_upload_task 
        >> load_s3_to_snowflake 
        >> transform_silver 
        >> aggregate_gold 
        >> end_pipeline
    )
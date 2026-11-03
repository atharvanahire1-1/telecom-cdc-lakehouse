-- ============================================================
-- 02_silver_transformations.sql
-- Telecom Lakehouse: Silver Layer Transformation & Deduplication
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE TELECOM_WH;
USE DATABASE TELECOM_LAKEHOUSE;
USE SCHEMA SILVER;

-- 1. Create Cleaned & Flattened Silver Table
-- Parses variant JSON into typed relational columns and adds audit metadata
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

-- 2. Transform, Deduplicate, and Ingest from Bronze
-- Uses QUALIFY ROW_NUMBER() to handle duplicate event IDs deterministically
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

-- 3. Verify Silver Transformation
SELECT COUNT(*) AS total_silver_records FROM silver_cdc_events;

-- Preview cleaned records with typed columns and usage deltas
SELECT 
    change_id,
    customer_id,
    operation,
    old_status,
    new_status,
    old_data_used,
    new_data_used,
    data_usage_delta,
    is_churn_event,
    silver_processed_at
FROM silver_cdc_events
ORDER BY change_id ASC
LIMIT 10;
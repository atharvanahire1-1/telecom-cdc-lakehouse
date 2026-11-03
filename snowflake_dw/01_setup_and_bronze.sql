-- 1. Context & Compute Setup
USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS TELECOM_WH
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS TELECOM_LAKEHOUSE;
USE DATABASE TELECOM_LAKEHOUSE;

-- 2. Create Schema Architecture (Medallion Layers)
CREATE SCHEMA IF NOT EXISTS BRONZE;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;

-- 3. File Formats & Internal Staging (Bronze Layer)
USE SCHEMA BRONZE;

CREATE OR REPLACE FILE FORMAT jsonl_format
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = FALSE
    ENABLE_OCTAL = FALSE;

CREATE OR REPLACE STAGE cdc_stage
    FILE_FORMAT = jsonl_format;

-- 4. Create Bronze Ingestion Table (Raw Variant Storage)
CREATE OR REPLACE TABLE raw_cdc_events (
    raw_payload VARIANT,
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file_name STRING,
    file_row_number NUMBER
);

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE TELECOM_WH;
USE DATABASE TELECOM_LAKEHOUSE;
USE SCHEMA BRONZE;

-- 1. Load data from CDC_STAGE into raw_cdc_events
COPY INTO raw_cdc_events (raw_payload, source_file_name, file_row_number)
FROM (
    SELECT 
        $1,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER
    FROM @cdc_stage
)
FILE_FORMAT = (FORMAT_NAME = 'jsonl_format')
ON_ERROR = 'CONTINUE';

-- 2. Verify row count
SELECT COUNT(*) AS total_bronze_records FROM raw_cdc_events;
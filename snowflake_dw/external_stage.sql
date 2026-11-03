USE ROLE ACCOUNTADMIN;
USE WAREHOUSE TELECOM_WH;
USE DATABASE TELECOM_LAKEHOUSE;
USE SCHEMA BRONZE;

CREATE OR REPLACE STAGE telecom_s3_stage
  URL = 's3://telecom-cdc-lakehouse-data/landing/'
  CREDENTIALS = (
    AWS_KEY_ID = 'abcd' --for security reasons
    AWS_SECRET_KEY = 'abcd'--for security reasons
'
  )
  FILE_FORMAT = (FORMAT_NAME = 'jsonl_format');

  LIST @telecom_s3_stage;
  USE ROLE ACCOUNTADMIN;
USE WAREHOUSE TELECOM_WH;
USE DATABASE TELECOM_LAKEHOUSE;
USE SCHEMA BRONZE;

-- Truncate existing raw records to test the S3 loading path cleanly
TRUNCATE TABLE raw_cdc_events;

-- Load directly from the external S3 stage
COPY INTO raw_cdc_events (raw_payload, source_file_name, file_row_number)
FROM (
    SELECT
        $1,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER
    FROM @telecom_s3_stage
)
FILE_FORMAT = (FORMAT_NAME = 'jsonl_format')
FORCE = TRUE
ON_ERROR = 'CONTINUE';

-- Verify row count
SELECT COUNT(*) AS total_bronze_records FROM raw_cdc_events;
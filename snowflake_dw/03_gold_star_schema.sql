-- ============================================================
-- 03_gold_star_schema.sql
-- Telecom Lakehouse: Gold Layer Star Schema for BI Analytics
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE TELECOM_WH;
USE DATABASE TELECOM_LAKEHOUSE;
USE SCHEMA GOLD;

-- 1. Dimension: Calendar / Date Dimension
CREATE OR REPLACE TABLE dim_date AS
WITH date_spine AS (
    SELECT 
        DATEADD(day, SEQ4(), '2026-01-01'::DATE) AS calendar_date
    FROM TABLE(GENERATOR(ROWCOUNT => 730)) -- Covers 2 years
)
SELECT
    TO_NUMBER(TO_CHAR(calendar_date, 'YYYYMMDD')) AS date_key,
    calendar_date,
    YEAR(calendar_date) AS year,
    QUARTER(calendar_date) AS quarter,
    MONTH(calendar_date) AS month,
    MONTHNAME(calendar_date) AS month_name,
    DAY(calendar_date) AS day_of_month,
    DAYOFWEEK(calendar_date) AS day_of_week,
    DAYNAME(calendar_date) AS day_name,
    CASE WHEN DAYOFWEEK(calendar_date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM date_spine;

-- 2. Dimension: Churn Severity & Usage Band
CREATE OR REPLACE TABLE dim_usage_band (
    band_key INT AUTOINCREMENT PRIMARY KEY,
    band_name VARCHAR(50),
    min_usage FLOAT,
    max_usage FLOAT
);

INSERT INTO dim_usage_band (band_name, min_usage, max_usage)
VALUES 
    ('Low Usage (< 1,000 MB)', 0, 1000),
    ('Medium Usage (1,000 - 5,000 MB)', 1000, 5000),
    ('High Usage (5,000 - 10,000 MB)', 5000, 10000),
    ('Heavy Usage (> 10,000 MB)', 10000, 999999);

-- 3. Fact Table: Churn & Mutation Events
CREATE OR REPLACE TABLE fact_churn_events AS
SELECT
    s.change_id AS churn_event_id,
    s.customer_id,
    TO_NUMBER(TO_CHAR(s.change_timestamp::DATE, 'YYYYMMDD')) AS date_key,
    s.change_timestamp AS event_timestamp,
    s.operation,
    s.old_status,
    s.new_status,
    s.old_data_used,
    s.new_data_used,
    s.data_usage_delta,
    s.is_churn_event,
    CASE 
        WHEN s.new_data_used < 1000 THEN 'Low Usage (< 1,000 MB)'
        WHEN s.new_data_used BETWEEN 1000 AND 5000 THEN 'Medium Usage (1,000 - 5,000 MB)'
        WHEN s.new_data_used BETWEEN 5000 AND 10000 THEN 'High Usage (5,000 - 10,000 MB)'
        ELSE 'Heavy Usage (> 10,000 MB)'
    END AS usage_segment
FROM TELECOM_LAKEHOUSE.SILVER.silver_cdc_events s;

-- 4. Analytical Verification Query
SELECT 
    usage_segment,
    COUNT(*) AS total_churn_events,
    ROUND(AVG(new_data_used), 2) AS avg_final_data_used,
    ROUND(SUM(new_data_used) / 1024, 2) AS total_data_consumed_gb
FROM fact_churn_events
WHERE is_churn_event = TRUE
GROUP BY usage_segment
ORDER BY total_churn_events DESC;
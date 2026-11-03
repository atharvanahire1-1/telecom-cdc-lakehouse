# 📡 Telecom CDC Lakehouse Pipeline

An automated Change Data Capture (CDC) Lakehouse data pipeline processing 48k+ telecom events. Orchestrated using Dockerized Apache Airflow, landed in AWS S3, and transformed using Snowflake's Medallion Architecture (Bronze ➔ Silver ➔ Gold).

---

## 🏗️ Architecture Overview

```mermaid
flowchart LR
    A[CDC Event Producer] -->|Simulated Mutations| B[Landing Stage]
    B -->|Airflow PythonOperator| C[(AWS S3 Bucket)]
    C -->|Snowflake COPY INTO| D[(Bronze Layer)]
    D -->|Deduplication & Parsing| E[(Silver Layer)]
    E -->|Star Schema Aggregation| F[(Gold Layer)]
    F --> G[BI & Analytics]
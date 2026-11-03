import sqlite3
import pandas as pd

DB_FILE = "telecom_dw.db"
CSV_FILE = "telecom_churn.csv"

def setup_database():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # 1. Drop existing tables if recreating
    cursor.execute("DROP TABLE IF EXISTS customers;")
    cursor.execute("DROP TABLE IF EXISTS cdc_log;")
    cursor.execute("DROP TRIGGER IF EXISTS trg_customer_cdc;")

    # 2. Operational OLTP Table
    cursor.execute("""
    CREATE TABLE customers (
        customer_id INTEGER PRIMARY KEY,
        telecom_partner TEXT,
        gender TEXT,
        age INTEGER,
        state TEXT,
        city TEXT,
        pincode INTEGER,
        date_of_registration TEXT,
        num_dependents INTEGER,
        estimated_salary INTEGER,
        calls_made INTEGER,
        sms_sent INTEGER,
        data_used REAL,
        status TEXT,
        churn_target INTEGER
    );
    """)

    # 3. CDC Log Table (captures row-level delta events)
    cursor.execute("""
    CREATE TABLE cdc_log (
        change_id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER,
        operation TEXT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        before_state TEXT,
        after_state TEXT,
        processed_flag INTEGER DEFAULT 0
    );
    """)

    # 4. Trigger to capture changes as JSON payloads
    cursor.execute("""
    CREATE TRIGGER trg_customer_cdc
    AFTER UPDATE ON customers
    FOR EACH ROW
    BEGIN
        INSERT INTO cdc_log (
            customer_id, 
            operation, 
            before_state, 
            after_state
        )
        VALUES (
            OLD.customer_id,
            'UPDATE',
            json_object(
                'status', OLD.status,
                'data_used', OLD.data_used,
                'calls_made', OLD.calls_made,
                'sms_sent', OLD.sms_sent
            ),
            json_object(
                'status', NEW.status,
                'data_used', NEW.data_used,
                'calls_made', NEW.calls_made,
                'sms_sent', NEW.sms_sent
            )
        );
    END;
    """)

    conn.commit()
    print("Database schema and CDC trigger created successfully.")

    # 5. Load and clean CSV into SQLite
    print(f"Loading data from {CSV_FILE}...")
    df = pd.read_csv(CSV_FILE)

    # Clean negative usage values
    df["data_used"] = df["data_used"].abs()

    # Initialize all subscribers as 'active' for OLTP baseline
    df["status"] = "active"

    # Retain original Kaggle churn flag for mutation replay
    df.rename(columns={"churn": "churn_target"}, inplace=True)

    # Ingest into SQLite
    df.to_sql("customers", conn, if_exists="append", index=False)
    conn.commit()

    # Validate row counts
    cursor.execute("SELECT COUNT(*) FROM customers;")
    total_customers = cursor.fetchone()[0]
    print(f"Total customers loaded into SQLite: {total_customers}")

    conn.close()

if __name__ == "__main__":
    setup_database()
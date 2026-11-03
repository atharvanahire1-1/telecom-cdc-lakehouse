import sqlite3
import random

DB_FILE = "telecom_dw.db"

def simulate_mutations():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # 1. Check existing churn target count
    cursor.execute("SELECT COUNT(*) FROM customers WHERE churn_target = 1 AND status = 'active';")
    target_count = cursor.fetchone()[0]

    if target_count == 0:
        print("No active churn-target customers found to mutate.")
        conn.close()
        return

    print(f"Found {target_count} target customers to transition to 'churned'.")

    # 2. Fetch customers designated to churn along with current usage
    cursor.execute("SELECT customer_id, data_used FROM customers WHERE churn_target = 1 AND status = 'active';")
    records = cursor.fetchall()

    # 3. Batch prepare updates with simulated usage drops
    update_data = []
    for customer_id, current_data in records:
        # Simulate final reduced usage before churn
        reduced_usage = round(max(0.0, current_data * random.uniform(0.1, 0.8)), 2)
        update_data.append((reduced_usage, customer_id))

    print("Executing batch UPDATE statements to trigger CDC captures...")

    # 4. Execute update in batches to trigger row-level CDC logging
    cursor.executemany("""
        UPDATE customers
        SET status = 'churned',
            data_used = ?
        WHERE customer_id = ?;
    """, update_data)

    conn.commit()

    # 5. Verify trigger output in cdc_log
    cursor.execute("SELECT COUNT(*) FROM cdc_log;")
    cdc_records = cursor.fetchone()[0]

    cursor.execute("SELECT customer_id, operation, before_state, after_state FROM cdc_log LIMIT 1;")
    sample_log = cursor.fetchone()

    print(f"Mutation simulation completed successfully.")
    print(f"Total CDC events captured in 'cdc_log': {cdc_records}")
    print(f"Sample CDC Event:\n  Customer ID: {sample_log[0]}\n  Operation: {sample_log[1]}\n  Before: {sample_log[2]}\n  After:  {sample_log[3]}")

    conn.close()

if __name__ == "__main__":
    simulate_mutations()
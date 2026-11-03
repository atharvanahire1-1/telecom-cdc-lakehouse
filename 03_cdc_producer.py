import os
import json
import sqlite3
from dotenv import load_dotenv
from confluent_kafka import Producer

# Load credentials from .env
load_dotenv()

DB_FILE = "telecom_dw.db"
LANDING_FILE = os.path.join("stream_landing", "cdc_event_stream.jsonl")

BOOTSTRAP_SERVERS = os.getenv("CONFLUENT_BOOTSTRAP_SERVERS")
API_KEY = os.getenv("CONFLUENT_API_KEY")
API_SECRET = os.getenv("CONFLUENT_API_SECRET")
TOPIC = os.getenv("KAFKA_TOPIC", "telecom.churn.events")

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")

def produce_cdc_stream():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # 1. Fetch unprocessed CDC entries
    cursor.execute("""
        SELECT change_id, customer_id, operation, timestamp, before_state, after_state
        FROM cdc_log
        WHERE processed_flag = 0
        ORDER BY change_id ASC;
    """)
    records = cursor.fetchall()

    if not records:
        print("No unprocessed CDC events found.")
        conn.close()
        return

    print(f"Found {len(records)} unprocessed CDC records.")

    # 2. Configure Confluent Kafka Producer
    conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': API_KEY,
        'sasl.password': API_SECRET,
        'client.id': 'telecom-cdc-producer'
    }
    producer = Producer(conf)

    # 3. Stream to Confluent and write to local landing
    os.makedirs("stream_landing", exist_ok=True)
    processed_ids = []

    with open(LANDING_FILE, "a", encoding="utf-8") as f_out:
        for row in records:
            change_id, cust_id, op, ts, before_s, after_s = row
            
            payload = {
                "change_id": change_id,
                "customer_id": cust_id,
                "operation": op,
                "timestamp": ts,
                "before": json.loads(before_s),
                "after": json.loads(after_s)
            }
            
            payload_str = json.dumps(payload)

            # Write locally
            f_out.write(payload_str + "\n")

            # Publish to Confluent Cloud Kafka
            producer.produce(
                topic=TOPIC,
                key=str(cust_id),
                value=payload_str.encode('utf-8'),
                callback=delivery_report
            )
            producer.poll(0)

            processed_ids.append((change_id,))

    print("Flushing outstanding messages to Confluent Cloud...")
    producer.flush()

    # 4. Mark records as processed in SQLite
    cursor.executemany("UPDATE cdc_log SET processed_flag = 1 WHERE change_id = ?;", processed_ids)
    conn.commit()

    print(f"Successfully produced and marked {len(processed_ids)} records as processed.")
    conn.close()

if __name__ == "__main__":
    produce_cdc_stream()
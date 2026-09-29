import json
import psycopg2
from kafka import KafkaConsumer

# 1. Connect to PostgreSQL
# Update 'password' to the password you set during PostgreSQL installation
conn = psycopg2.connect(
    host="127.0.0.1",
    database="postgres",
    user="postgres",
    password="",
    port=5432
)
cursor = conn.cursor()

# 2. Create target table if it does not exist
cursor.execute("""
CREATE TABLE IF NOT EXISTS ecommerce_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    amount_inr NUMERIC(10, 2),
    order_status VARCHAR(20),
    created_at TIMESTAMP
);
""")
conn.commit()
print("Connected to PostgreSQL. Table 'ecommerce_orders' is ready.")

# 3. Connect to Kafka Consumer
consumer = KafkaConsumer(
    'orders.raw',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Listening for messages on topic 'orders.raw'...")

# 4. Ingest stream into database
for message in consumer:
    data = message.value
    insert_query = """
    INSERT INTO ecommerce_orders (order_id, amount_inr, order_status, created_at)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (order_id) DO NOTHING;
    """
    cursor.execute(insert_query, (
        data['order_id'],
        data['amount_inr'],
        data['order_status'],
        data['timestamp']
    ))
    conn.commit()
    print(f"Stored in DB -> Order: {data['order_id']} | INR {data['amount_inr']} | {data['order_status']}")
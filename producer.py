import uuid
import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

# Initialize Kafka Producer
# Ensure your local Kafka broker is running on localhost:9092
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'orders.raw'

print(f"Starting to send real-time e-commerce orders to '{topic_name}'...\n")

try:
    order_counter = 1000
    while True:
        # Generating raw data figures (exact INR amounts, no percentages)
        order_payload = {
            'order_id': f"ORD-{uuid.uuid4().hex[:6]}",
            "amount_inr": round(random.uniform(250.0, 5500.0), 2),
            "order_status": random.choice(["success", "success", "failed", "pending"]),
            "timestamp": datetime.now().isoformat()
        }
        
        # Send to Kafka topic
        producer.send(topic_name, order_payload)
        
        # Terminal output confirming success
        print(f"Sent: {json.dumps(order_payload)}")
        
        order_counter += 1
        time.sleep(1)  # Sends 1 message per second

except KeyboardInterrupt:
    print("\nProducer stopped by user.")
finally:
    producer.close()
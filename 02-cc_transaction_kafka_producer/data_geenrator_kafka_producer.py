from confluent_kafka import Producer
import csv
import time
import os
from datetime import datetime


# Kafka broker settings
KAFKA_BROKER = "localhost:9092,localhost:9093,localhost:9094"
KAFKA_TOPIC = 'card_transaction'

# Configure Kafka producer
producer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': 'card-transaction'
}
producer = Producer(producer_conf)

# Function to read CSV and send to Kafka
def stream_customer_data(file_path):
    if not os.path.exists(csv_file_path):
        raise FileNotFoundError(f"File not found: {csv_file_path}")
    with open(file_path, mode='r', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file)
        for row in reader:
            row['transaction_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            message = str(row)
            producer.produce(KAFKA_TOPIC, key=row.get('cc_num', 'default_key'), value=message)
            producer.flush()
            print(f"Produced: {message}")
            time.sleep(1)

if __name__ == '__main__':
    base_dir = "C:\\Users\\Viveka\\Viveka\\Upskill\\2025\\Fraud detection project\\fraud_detection_de_project"
    csv_file_path = os.path.join(base_dir, "dataset", "customer_details.csv")
    stream_customer_data(csv_file_path)

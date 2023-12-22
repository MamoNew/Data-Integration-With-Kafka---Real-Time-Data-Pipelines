# UploadDataset_to_kafka_procedure.py

# Import necessary libraries and modules
import time
from kafka import KafkaProducer
import pandas as pd

# Set up Kafka producer
bootstrap_servers = 'localhost:9092'
topic = 'traffic_kafka_data_topic'
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Load data from Traffic.csv
csv_path = "/home/mamo/project_directory/Traffic.csv"
traffic_data = pd.read_csv(csv_path)

# Run the producer in an infinite loop
iteration = 1
while True:
    print(f"Iteration {iteration}/Infinity: Data from Traffic.csv pushed to Kafka successfully.")

    # Send data to Kafka
    for _, data_row in traffic_data.iterrows():
        producer.send(topic, value=data_row.to_json().encode('utf-8'))

    # Pause for a while before the next iteration (adjust the sleep time as needed)
    time.sleep(10)

    # Increment iteration counter
    iteration += 1

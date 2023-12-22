# KafkaMessageProducerWithLimit.py

# Import necessary libraries and modules
import pandas as pd
from confluent_kafka import Producer
import time

# Read the CSV file into a Pandas DataFrame
df = pd.read_csv('Traffic.csv')

# Kafka producer configuration
producer_config = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker address
}

# Create a Kafka producer instance
producer = Producer(producer_config)

# Kafka topic to which you want to push messages
kafka_topic = 'traffic_kafka_data_topic'  # Kafka topic name

# The maximum duration for the producer to run (in seconds)
# It takes data every 10 seconds, consider adjusting based on your requirements
max_duration_seconds = 10

# Record the start time
start_time = time.time()

# Run the producer for a maximum duration
while time.time() - start_time < max_duration_seconds:
    # Convert each row to JSON and push to Kafka
    for _, row in df.iterrows():
        message_value = row.to_json()
        producer.produce(kafka_topic, value=message_value)

    # Wait for any outstanding messages to be delivered and delivery reports to be received
    producer.flush()

    # Print a message indicating that messages have been sent
    print(f"Data from Traffic.csv pushed to Kafka successfully.")

    # Sleep for 1 second before sending the next batch of messages
    time.sleep(1)

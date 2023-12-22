# UploadDataset_to_kafka_procedure.py

# Import necessary modules and classes
from confluent_kafka import Consumer, KafkaError

# Define Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',  # Kafka bootstrap servers
    'group.id': 'your_consumer_group',  # Consumer group ID
    'auto.offset.reset': 'earliest',  # Configuration determines what to do when there is no initial offset in Kafka or if the current offset does not exist any longer on the server.
}

# Create a Kafka consumer instance
consumer = Consumer(consumer_config)

# The data is coming from 'traffic_kafka_data_topic' and is shown up
kafka_topic = 'traffic_kafka_data_topic'  # Kafka topic name
consumer.subscribe([kafka_topic])

# Poll for messages
while True:
    msg = consumer.poll(1.0)  # The timeout (in seconds) for the poll operation.

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            # End of partition event
            continue
        else:
            print(msg.error())
            break

    # Print the received message value
    # Convert the raw binary data of the Kafka message payload into a Unicode string
    print('Received message: {}'.format(msg.value().decode('utf-8')))

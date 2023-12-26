## Traffic Data Processing with Kafka and Spark 🚗📊

This project demonstrates a pipeline for real-time traffic data processing using Kafka and Spark. The system includes components for data generation, Kafka message production, Spark Structured Streaming, and Spark ML integration.

------------------------------------------
This PySpark script establishes a structured streaming pipeline integrated with Kafka for real-time traffic data analysis. The code defines functions for creating a Spark session, training a machine learning model (Logistic Regression in this case), and executing the main data processing logic. It reads static data from a CSV file and streaming data from a Kafka topic, parsing and filtering it based on specified conditions. The machine learning model is either loaded from a saved location or trained on the static and streaming data. Finally, the results are written to the console using structured streaming. The script handles keyboard interruptions to gracefully stop the streaming query.
-------------------------------------
### 1. *Traffic Data Generation 🚦*

The first part of the project involves generating simulated traffic data from a CSV file. The Python script TrafficDataGenerator.py utilizes Pandas to read a CSV file and push each row as a JSON message to a Kafka topic.

![Image](https://raw.githubusercontent.com/MaMo77570/Traffic-Data-Processing-with-Kafka-and-Spark-/main/1.png)

----------------------------------------------
### 2. *Spark Structured Streaming 🌟*

The Spark Structured Streaming script (StructuredStreaming.py) subscribes to the Kafka topic, processes incoming JSON messages, and applies streaming analytics. The processed data is then filtered based on a condition (e.g., Total > 50) and displayed on the console.

![Spark ML Integration](https://github.com/MaMo77570/Traffic-Data-Processing-with-Kafka-and-Spark-/blob/main/IMG-20231218-WA0048.jpg)

*********************************************************
![Spark ML Integration](https://raw.githubusercontent.com/MaMo77570/Traffic-Data-Processing-with-Kafka-and-Spark-/main/IMG-20231218-WA0042.jpg)

----------------------------------------------------------------------



![Image](https://raw.githubusercontent.com/MaMo77570/Traffic-Data-Processing-with-Kafka-and-Spark-/main/IMG-20231218-WA0044.jpg)





### 3. *Spark ML Integration 🤖*

The next step involves machine learning integration using Spark ML. The script SparkMLIntegration.py reads streaming data and combines it with static data for training a Linear Regression model. The model is then used to make predictions on the streaming data, and the results are displayed.

--------------------------------------------------------------------------------------------


### 4. *Kafka Message Producer with Limit ⏰*

To control the duration of data production, the script KafkaMessageProducerWithLimit.py sends data to Kafka for a specified duration. This can be useful for testing and limiting the amount of data pushed to Kafka.

![Spark ML Integration](https://raw.githubusercontent.com/MaMo77570/Traffic-Data-Processing-with-Kafka-and-Spark-/main/IMG-20231218-WA0046.jpg)


--------------------------------------------------------------------------------------

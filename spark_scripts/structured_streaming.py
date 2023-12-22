# structured_streaming.py

# Import necessary PySpark and related libraries and modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType


# Function to create a Spark session
def create_spark_session():
    return (
        SparkSession.builder.appName("KafkaStructuredStreaming")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .getOrCreate()
    )

# Main function for structured streaming
def main():
    spark = create_spark_session()

    # Kafka configuration
    kafka_bootstrap_servers = "localhost:9092"
    kafka_topic = "traffic_kafka_data_topic"

    # Define schema for the streaming data
    schema = StructType().add("Time", StringType()).add("Date", IntegerType()) \
                        .add("Day of the week", StringType()).add("CarCount", IntegerType()) \
                        .add("BikeCount", IntegerType()).add("BusCount", IntegerType()) \
                        .add("TruckCount", IntegerType()).add("Total", IntegerType()) \
                        .add("Traffic Situation", StringType())

    # Read streaming data from Kafka
    raw_stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .load()
    )

    # Select the 'value' column and cast it as a STRING
    json_stream_df = raw_stream_df.selectExpr("CAST(value AS STRING) as value")

    # Parse the JSON values and select the 'data' column
    parsed_stream_df = json_stream_df.select(
        from_json("value", schema).alias("data")
    ).select("data.*")

    # Add a 'label' column (you may adjust the condition based on your requirement)
    labeled_stream_df = parsed_stream_df.withColumn("label", col("Total").cast(IntegerType()))

    # Transformations or filtering if needed
    processed_stream_df = labeled_stream_df.filter("Total > 50")

    # Start the streaming query and write to the console
    query = (
        processed_stream_df.writeStream.outputMode("append")
        .format("console")
        .start()
    )

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        # Stop the streaming query on manual interruption
        query.stop()

# Run the main function if the script is executed directly
if __name__ == "__main__":
    main()

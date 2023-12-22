# SparkSQL.py
# Import necessary PySpark and related libraries and modules
from cffi.model import StructType
from jeepney.low_level import StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import IntegerType


# Function to create a Spark session
def create_spark_session():
    return SparkSession.builder.appName("SparkSQLExample").getOrCreate()


# Function to process streaming data using Spark SQL
def process_spark_sql(streaming_df):
    # Create a temporary view for the streaming DataFrame
    streaming_df.createOrReplaceTempView("streaming_data")

    # Execute a SQL query on the streaming data (select values > 50)
    sql_result = spark.sql("SELECT * FROM streaming_data WHERE value > 50")

    # Show the result of the SQL query
    sql_result.show()


# Main function for Spark SQL processing
def main_spark_sql():
    global spark
    spark = create_spark_session()

    # Kafka configuration
    kafka_bootstrap_servers = "localhost:9092"
    kafka_topic = "traffic_kafka_data_topic"

    # Define the schema for the streaming data
    schema = StructType().add("timestamp", StringType()).add("value", IntegerType())

    # Read streaming data from Kafka
    streaming_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .load()
    )

    # Select the 'value' column and cast it as a STRING
    json_stream_df = streaming_df.selectExpr("CAST(value AS STRING) as value")

    # Parse the JSON values and select the 'data' column
    parsed_stream_df = json_stream_df.select(
        from_json("value", schema).alias("data")
    ).select("data.*")

    # Process the streaming data using Spark SQL
    process_spark_sql(parsed_stream_df)

    # Start the Spark Streaming context
    spark.streams.awaitAnyTermination()


# Run the main function if the script is executed directly
if __name__ == "__main__":
    main_spark_sql()

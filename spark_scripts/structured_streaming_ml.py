# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from pyspark.sql.types import StructType, StringType, IntegerType
# from pyspark.ml import PipelineModel
# #from pyspark.ml.feature import VectorAssembler
# from pyspark.ml import Pipeline
#
# def create_spark_session():
#     return (
#         SparkSession.builder.appName("KafkaStructuredStreaming")
#         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
#         .getOrCreate()
#     )
#
# def train_ml_model(static_data):
#     # Train a machine learning model using a Logistic Regression classifier
#     from pyspark.ml.classification import LogisticRegression
#     from pyspark.ml.feature import VectorAssembler
#     from pyspark.ml import Pipeline
#
#     #a "label" column in my static data
#     static_data_with_label = static_data.withColumn("label", col("Total").cast(IntegerType()))
#     # Create a feature vector using VectorAssembler
#     assembler = VectorAssembler(inputCols=["CarCount", "BikeCount", "BusCount", "TruckCount"], outputCol="Cfeatures")
#     lr = LogisticRegression(featuresCol="Cfeatures", labelCol="label")
#
#     # Create a pipeline for the machine learning model
#     # ()()()()()())()()()()))()()())()()()()()()()()()()()()()()()())(
#
#     pipeline = Pipeline(stages=[assembler, lr])
#     # pipeline.save("/home/mamo/project_directory/spark_scripts/logistic_regression_model/metadata")
#     model = pipeline.fit(static_data_with_label)  # Use the static data with the label
#     model.save("/home/mamo/project_directory/spark_scripts/logistic_regression_model/metadata")
#     return model
#
# def main():
#     # Create a Spark session
#     spark = create_spark_session()
#     # Kafka configuration
#     kafka_bootstrap_servers = "localhost:9092"
#     kafka_topic = "traffic_kafka_data_topic"
#
#     # Define the schema for the data
#     schema = StructType().add("Time", StringType()).add("Date", IntegerType()) \
#                         .add("Day of the week", StringType()).add("CarCount", IntegerType()) \
#                         .add("BikeCount", IntegerType()).add("BusCount", IntegerType()) \
#                         .add("TruckCount", IntegerType()).add("Total", IntegerType()) \
#                         .add("Traffic Situation", StringType())
#
#     # Read static data from a CSV file
#     static_data_path = "/home/mamo/project_directory/spark_scripts/Traffic.csv"
#     static_data = spark.read.csv(static_data_path, header=True, schema=schema)
#
#     # Read streaming data from Kafka
#     raw_stream_df = (
#         spark.readStream.format("kafka")
#         .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
#         .option("subscribe", kafka_topic)
#         .load()
#     )
#
#     json_stream_df = raw_stream_df.selectExpr("CAST(value AS STRING) as value")
#
#     # Parse the JSON data and select fields
#     parsed_stream_df = json_stream_df.select(
#         from_json("value", schema).alias("data")
#     ).select("data.*")
#
#     # Add a "label" column to the streaming data
#     labeled_stream_df = parsed_stream_df.withColumn("label", col("Total").cast(IntegerType()))
#
#     # Filter the streaming data based on a condition
#     processed_stream_df = labeled_stream_df.filter("Total > 50")
#
#     # Train or load the machine learning model
#     # ml_model = train_ml_model(static_data)
# #()()()()()())()()()()))()()())()()()()()()()()()()()()()()()())(
#     ml_model= PipelineModel.load("/home/mamo/project_directory/spark_scripts/logistic_regression_model/metadata")
#
#     # Use the machine learning model
#     ml_result = ml_model.transform(processed_stream_df)
#
#     # Write the results to the console in append mode
#     query = (
#         ml_result.writeStream.outputMode("append")
#         .format("console")
#         .start()
#     )
#
#     try:
#         query.awaitTermination()
#     except KeyboardInterrupt:
#         query.stop()
#
# #checks whether the script is being run as the main program. If it is, the code inside the if block is executed.
# if __name__ == "__main__":
#     main()
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel


def create_spark_session():
    return (
        SparkSession.builder.appName("KafkaStructuredStreaming")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .getOrCreate()
    )

def train_ml_model(static_data, training_data):
    # Your machine learning model training logic goes here
    # Example using Logistic Regression:
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml import Pipeline

    # Add a "label" column to your static data
    static_data_with_label = static_data.withColumn("label", col("Total").cast(IntegerType()))

    assembler = VectorAssembler(inputCols=["CarCount", "BikeCount", "BusCount", "TruckCount"], outputCol="Cfeatures")
    lr = LogisticRegression(featuresCol="Cfeatures", labelCol="label")

    pipeline = Pipeline(stages=[assembler, lr])
    model = pipeline.fit(static_data_with_label)  # Use the static data with the label

    return model

def main():
    spark = create_spark_session()

    kafka_bootstrap_servers = "localhost:9092"
    kafka_topic = "traffic_kafka_data_topic"

    schema = StructType().add("Time", StringType()).add("Date", IntegerType()) \
                        .add("Day of the week", StringType()).add("CarCount", IntegerType()) \
                        .add("BikeCount", IntegerType()).add("BusCount", IntegerType()) \
                        .add("TruckCount", IntegerType()).add("Total", IntegerType()) \
                        .add("Traffic Situation", StringType())

    static_data_path = "/home/mamo/project_directory/spark_scripts/Traffic.csv"
    static_data = spark.read.csv(static_data_path, header=True, schema=schema)

    raw_stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .load()
    )

    json_stream_df = raw_stream_df.selectExpr("CAST(value AS STRING) as value")

    parsed_stream_df = json_stream_df.select(
        from_json("value", schema).alias("data")
    ).select("data.*")

    labeled_stream_df = parsed_stream_df.withColumn("label", col("Total").cast(IntegerType()))

    processed_stream_df = labeled_stream_df.filter("Total > 50")

    # Train or load the machine learning model
    # ml_model = train_ml_model(static_data, processed_stream_df)
    ml_model= PipelineModel.load("/home/mamo/project_directory/spark_scripts/logistic_regression_model/metadata")


    # Use the machine learning model
    ml_result = ml_model.transform(processed_stream_df)

    query = (
        ml_result.writeStream.outputMode("append")
        .format("console")
        .start()
    )

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()

if __name__ == "__main__":
    main()
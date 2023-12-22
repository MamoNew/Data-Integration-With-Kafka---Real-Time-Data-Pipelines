# # ml.py
# from pyspark.sql.functions import col
# from pyspark.ml.classification import LogisticRegression
# from pyspark.ml.feature import VectorAssembler
# from pyspark.ml import Pipeline
# from pyspark.sql.types import IntegerType
# from pyspark.ml import PipelineModel
# def train_ml_model(static_data):
#     # "label" column in static data
#     static_data_with_label = static_data.withColumn("label", col("Total").cast(IntegerType()))
#
#     # Create a feature vector using VectorAssembler
#     assembler = VectorAssembler(inputCols=["CarCount", "BikeCount", "BusCount", "TruckCount"], outputCol="Cfeatures")
#     lr = LogisticRegression(featuresCol="Cfeatures", labelCol="label")
#
#     # Create a pipeline for the machine learning model
#     pipeline = Pipeline(stages=[assembler, lr])
#
#     # Train and save the machine learning model
#     model = pipeline.fit(static_data_with_label)
#     model.save("/home/mamo/project_directory/spark_scripts/logistic_regression_model/metadata")
#     return model
#
# if __name__ == "__main__":
#     from pyspark.sql import SparkSession
#     from pyspark.sql.types import StructType, StringType, IntegerType
#
#     def create_spark_session():
#         return (
#             SparkSession.builder.appName("MLTraining")
#             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
#             .getOrCreate()
#         )
#
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
#     static_data = create_spark_session().read.csv(static_data_path, header=True, schema=schema)
#
#     # Train the machine learning model
#     train_ml_model(static_data)
#------------------------------------------------------------------------------------------------
# def train_ml_model(static_data):
#     # "label" column in static data
#     static_data_with_label = static_data.withColumn("label", col("Total").cast(IntegerType()))
#
#     # Create a feature vector using VectorAssembler
#     assembler = VectorAssembler(inputCols=["CarCount", "BikeCount", "BusCount", "TruckCount"], outputCol="Cfeatures")
#     lr = LogisticRegression(featuresCol="Cfeatures", labelCol="label")
#
#     # Create a pipeline for the machine learning model
#     pipeline = Pipeline(stages=[assembler, lr])
#
#     # Train and save the machine learning model
#     model = pipeline.fit(static_data_with_label)
#     model.save("/home/mamo/project_directory/spark_scripts/logistic_regression_model/metadata")
#
#     # Print the output of the machine learning model without truncation
#     ml_result = model.transform(static_data_with_label)
#     ml_result.show(truncate=False, n=ml_result.count())
#
#     return model
#------------------------------------------------------------------------------------------------

# ml.py
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, expr
def train_ml_model(static_data):
    # "label" column in static data
    static_data_with_label = static_data.withColumn("label", col("Total").cast(IntegerType()))

    # Create a feature vector using VectorAssembler
    assembler = VectorAssembler(inputCols=["CarCount", "BikeCount", "BusCount", "TruckCount"], outputCol="Cfeatures")
    lr = LogisticRegression(featuresCol="Cfeatures", labelCol="label")

    # Create a pipeline for the machine learning model
    pipeline = Pipeline(stages=[assembler, lr])

    # Train and save the machine learning model
    model = pipeline.fit(static_data_with_label)
    model.save("/home/mamo/project_directory/spark_scripts/logistic_regression_model/metadata")

    # Print the output of the machine learning model without truncation
    ml_result = model.transform(static_data_with_label)
    # ml_result.show(truncate=False, n=ml_result.count())

    return model

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StringType, IntegerType

    def create_spark_session():
        return (
            SparkSession.builder.appName("MLTraining")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
            .config("spark.driver.host", "localhost")  # Set the driver host to suppress the hostname warning
            .config("spark.ui.reverseProxy", "true")  # Enable reverse proxy for Spark UI
            .config("spark.ui.reverseProxyUrl", "http://localhost:4040")  # Set the reverse proxy URL
            .getOrCreate()
        )

    # Kafka configuration
    kafka_bootstrap_servers = "localhost:9092"
    kafka_topic = "traffic_kafka_data_topic"

    # Define the schema for the data
    schema = StructType().add("Time", StringType()).add("Date", IntegerType()) \
        .add("Day of the week", StringType()).add("CarCount", IntegerType()) \
        .add("BikeCount", IntegerType()).add("BusCount", IntegerType()) \
        .add("TruckCount", IntegerType()).add("Total", IntegerType()) \
        .add("Traffic Situation", StringType())

    # Read static data from a CSV file
    static_data_path = "/home/mamo/project_directory/spark_scripts/Traffic.csv"
    static_data = create_spark_session().read.csv(static_data_path, header=True, schema=schema)

    # Train the machine learning model
    train_ml_model(static_data)

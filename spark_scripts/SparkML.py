# Import necessary PySpark libraries and modules
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

from spark_scripts.pullMessage_ML import assembler, lr


# Function to create a Spark session
def create_spark_session():
    return SparkSession.builder.appName("SparkMLApp").getOrCreate()


# Function to process data using Spark ML
def process_spark_ml(sql_result):
    # Define feature columns for the machine learning model
    feature_columns = ["CarCount", "BikeCount", "BusCount", "TruckCount", "Total"]

    # Create a VectorAssembler to assemble features into a single vector
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

    # Create a Logistic Regression model
    logistic_regression = LogisticRegression(featuresCol="features", labelCol="Total")

    # Create a machine learning pipeline with the VectorAssembler and Logistic Regression model
    ml_pipeline = Pipeline(stages=[assembler, logistic_regression])

    # Fit the pipeline to the input DataFrame
    ml_model = ml_pipeline.fit(sql_result)

    # Transform the input DataFrame with the fitted pipeline to obtain predictions
    ml_result = ml_model.transform(sql_result)

    # Show the resulting DataFrame with predictions
    ml_result.show()


# Function to analyze streaming data using Spark ML
def analyze_streaming_data(streaming_data):
    # Define feature columns for the machine learning model
    feature_columns = ["CarCount", "BikeCount", "BusCount", "TruckCount", "Total"]

    # Create a VectorAssembler to create a "features" column in the streaming DataFrame
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

    # Add a "prediction" column to the streaming DataFrame
    streaming_data_with_prediction = streaming_data.withColumn("features", expr("struct(*)"))
    streaming_data_with_prediction = assembler.transform(streaming_data_with_prediction)

    # Create a Logistic Regression model
    logistic_regression = LogisticRegression(featuresCol="features", labelCol="Total")

    # Fit the logistic regression model to the streaming data
    ml_model = logistic_regression.fit(streaming_data_with_prediction)

    # Display predictions for the streaming data
    predictions = ml_model.transform(streaming_data_with_prediction)
    predictions.select("timestamp", "Total", "prediction").show()


# Main function for Spark ML processing
def main_spark_ml(static_data_with_label=None):
    # Create a Spark session
    spark = create_spark_session()

    # Define the schema for the sample streaming data
    schema = ["label", "timestamp", "CarCount", "BikeCount", "BusCount", "TruckCount", "Total", "Traffic Situation"]

    pipeline = Pipeline(stages=[assembler, lr])
    pipeline.save("/home/mamo/project_directory/spark_scripts/logistic_regression_model/metadata")
    model = pipeline.fit(static_data_with_label)  # Use the static data with the label
    return model
    # Create a DataFrame from the sample data
    sql_result = spark.createDataFrame(data, schema)

    # Process the data using Spark ML
    process_spark_ml(sql_result)


# Run the main function if the script is executed directly
if __name__ == "__main__":
    main_spark_ml()

# PullMessage_ML.py

# Import necessary PySpark libraries and modules
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.sql.functions import col

# Step 1: Create a Spark session
spark = SparkSession.builder.appName("TrafficDataAnalysis").getOrCreate()

# Step 2: Load the CSV data into a DataFrame
csv_path = "/home/mamo/project_directory/Traffic.csv"
traffic_data = spark.read.csv(csv_path, header=True, inferSchema=True)

# Step 3: Explore and preprocess the data (customize as needed)
feature_cols = ['CarCount', 'BikeCount', 'BusCount', 'TruckCount', 'Total', 'Traffic Situation']

# Assemble features into a vector column
assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
traffic_data = assembler.transform(traffic_data).select('features', 'label')

# Step 4: Split the data into training and testing sets
(train_data, test_data) = traffic_data.randomSplit([0.8, 0.2], seed=123)

# Step 5: Define and train a machine learning model (Linear Regression in this example)
lr = LinearRegression(featuresCol='features', labelCol='label', predictionCol='prediction')
pipeline = Pipeline(stages=[lr])
model = pipeline.fit(train_data)

# Step 6: Make predictions on the test data
predictions = model.transform(test_data)

# Step 7: Show the predictions
predictions.select("features", "label", "prediction").show()

# Step 8: Perform additional analysis or evaluation as needed

# Step 9: Stop the Spark session
spark.stop()

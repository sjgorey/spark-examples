import os
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("ExampleJob").getOrCreate()

# Create a simple DataFrame
data = [("Alice", 34), ("Bob", 45), ("Charlie", 29), ("ddd", 44), ("sss", 55)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Show DataFrame
df.show()

# Stop Spark session
spark.stop()

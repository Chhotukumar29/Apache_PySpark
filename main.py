import os
from pyspark.sql.functions import col
from pyspark.sql import SparkSession

# Print current working dir
print("Current Working Directory:", os.getcwd())

# Create Spark session
spark = SparkSession.builder \
    .appName("DataProcessing") \
    .getOrCreate()

# file path of data
file_path = r"/config/workspace/data/weatherHistory.csv"

# Checking file exists or not at given path
if not os.path.exists(file_path):
    print(f"File not found: {file_path}")
else:
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # Handling missing values 
    df = df.dropna()
    df.show() 

    # Remove Duplicates
    df = df.dropDuplicates()
    print(df) 

    # Columns name
    print(df.columns)

    # Data Transformation
    df = df.withColumn("Temperature (C)", col("Temperature (C)").cast("Integer"))
    df = df.withColumnRenamed("Formatted Date", "Date")
    df = df.withColumnRenamed("Loud Cover", "Cloud Cover")

    # Data Aggregation
    aggregated_df = df.groupBy("Summary").agg({"Temperature (C)": "avg", "Humidity": "avg", "Wind Speed (km/h)": "avg"})
    aggregated_df = aggregated_df.withColumnRenamed("avg(Temperature (C))", "Avg_Temperature")
    aggregated_df = aggregated_df.withColumnRenamed("avg(Humidity)", "Avg_Humidity")
    aggregated_df = aggregated_df.withColumnRenamed("avg(Wind Speed (km/h))", "Avg_Wind_Speed")
    aggregated_df.show() 

    # Optimization
    df.cache()  # Caching

    df = df.repartition(10, "Summary")  # Partitioning

    df.show()

# Stop the Spark session
spark.stop()


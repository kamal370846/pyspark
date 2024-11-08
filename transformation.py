import pyspark
from pyspark.sql import SparkSession

# Initialize Spark session
sparkSession = SparkSession.builder.appName("transform").getOrCreate()

# Define file path
input_file_path = "data.txt"

# Read file into an RDD
lines = sparkSession.read.text(input_file_path).rdd.map(lambda r: r[0])

# Print original lines in the file
print("Original lines:", lines.collect())

# Filter out empty or whitespace-only lines
cleanlines = lines.filter(lambda line: line.strip())

# Print cleaned lines
print("Cleaned lines:", cleanlines.collect())
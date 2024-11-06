import pyspark
from pyspark.sql import SparkSession
sparkSession=SparkSession.builder.appName("transform").getOrCreate()
input_file_path = "data.txt"
lines = sparkSession.read.text(input_file_path).rdd.map(lambda r: r[0])
print(lines)
cleanlines= lines.filter(lambda line: line.strip())
print(cleanlines)
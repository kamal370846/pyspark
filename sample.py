import os
import sys
# os.environ["PYSPARK_PYTHON"] = "/opt/homebrew/bin/python3.9"
# os.environ["PYSPARK_DRIVER_PYTHON"] = "/opt/homebrew/bin/python3.9"

# # Check and print the PYSPARK_DRIVER_PYTHON and PYSPARK_PYTHON environment variables
# print("PYSPARK_DRIVER_PYTHON:", os.environ.get("PYSPARK_DRIVER_PYTHON"))
# print("PYSPARK_PYTHON:", os.environ.get("PYSPARK_PYTHON"))

# Set the environment variables if they are not set uncomment below if it is not python3.9
# if not os.environ.get("PYSPARK_DRIVER_PYTHON"):
#     os.environ["PYSPARK_DRIVER_PYTHON"] = "/opt/homebrew/bin/python3.9"  # Replace with the path to your Python 3.9 executable
# if not os.environ.get("PYSPARK_PYTHON"):
#     os.environ["PYSPARK_PYTHON"] = "/opt/homebrew/bin/python3.9"  # Replace with the path to your Python 3.9 executable

# print("PYSPARK_DRIVER_PYTHON:", os.environ.get("PYSPARK_DRIVER_PYTHON"))
# print("PYSPARK_PYTHON:", os.environ.get("PYSPARK_PYTHON"))

from pyspark.sql import SparkSession
# spark = SparkSession.builder \
#     .appName("ExampleApp") \
#     .config("spark.yarn.appMasterEnv.PYSPARK_PYTHON", "/opt/homebrew/bin/python3.9") \
#     .config("spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON", "/opt/homebrew/bin/python3.9") \
#     .config("spark.executorEnv.PYSPARK_PYTHON", "/opt/homebrew/bin/python3.9") \
#     .config("spark.executorEnv.PYSPARK_DRIVER_PYTHON", "/opt/homebrew/bin/python3.9") \
#     .getOrCreate()

# Create an RDD to check Python versions on workers
# rdd = spark.sparkContext.parallelize(range(1))
# python_versions = rdd.map(lambda x: sys.version).collect()
# print("Python versions on workers:", python_versions)


spark=SparkSession.builder.getOrCreate()
# sc=spark.read.csv('/Users/KA370846/learning/sample.csv')
# delimiter=[',','*']
# sc=spark.read.option("header",True).option("delimiter", "delimiter[0]").csv('sample.csv')
#sc.show()
# sc_split=sc.rdd.flatMap(lambda x: x.value.split(','))
# output=sc_split.collect()
# print(output)

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
csvText = spark.read.text('sample.csv')
rows = csvText.rdd.flatMap(lambda line: line.value.split(','))
schema = StructType([
        StructField("Name", StringType(), True),
        StructField("Age", IntegerType(), True),
        StructField("City", StringType(), True)
    ])

csvData = spark.read \
        .option("delimiter", "*") \
        .option("header", "true") \
        .schema(schema) \
        .csv(rows)
csvData.show()
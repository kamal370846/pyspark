import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("transform").getOrCreate()
from pyspark.sql.functions import when, col

# Sample data
data = [("ProductA", 30),
        ("ProductB", 75),
        ("ProductC", 110)]

# Define the schema
schema = ["ProductName", "Price"]

# Create the DataFrame
productsData = spark.createDataFrame(data, schema)

# Use when and otherwise to categorize product prices
result_df = productsData.withColumn("PriceCategory",
    when(col("Price") < 50, "Low")
    .when((col("Price") >= 50) & (col("Price") < 100), "Medium")
    .otherwise("High")
)

result_df.show()
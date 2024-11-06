# from pyspark import SparkContext

# # Initialize SparkContext
# sc = SparkContext("local", "Map Transformation Example")

# # Create an example RDD
# rdd = sc.parallelize([1, 2, 3, 4, 5])

# # Apply the map transformation to square each element
# squared_rdd = rdd.map(lambda x: x ** 2)

# # Collect and print the results
# output = squared_rdd.collect()
# print(output)  # Output: [1, 4, 9, 16, 25]

# # Stop the SparkContext
# sc.stop()

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("transformation").getOrCreate()
sc=spark.sparkContext
rdd=sc.parallelize([1,2,3,4,5,6,7,9,10])
squared_rdd=rdd.map(lambda x: x**2)
output=squared_rdd.collect()
print(output)
sc.stop()


from pyspark import SparkContext
sc=SparkContext("local","trnsform egs")
rdd=sc.parallelize([1,2,3,4,5,6,7,8,9,10])
clean=rdd.filter(lambda x:x%3==0)
#squr_rdd=rdd.map(lambda x: x**2)
output=clean.collect()
print(output)
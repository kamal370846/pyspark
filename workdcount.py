import pyspark
#inputInteger=list(range(1,10))
#integerRdd=sc.parallelize(inputInteger)

from pyspark.sql import SparkSession
#from operator import add
spark = SparkSession.builder.appName("test").getOrCreate()
input_file_path = "data.txt"
lines = spark.read.text(input_file_path).rdd.map(lambda r: r[0])
words = lines.flatMap(lambda line: line.split())
word_counts = words.map(lambda word: (word, 1))

word_counts = word_counts.reduceByKey(lambda x, y: x + y)
result = word_counts.collect()
for word, count in result:
    print(f"{word}: {count}")
spark.stop()    

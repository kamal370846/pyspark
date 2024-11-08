from pyspark.sql import SparkSession

# Initialize Spark session
sc = SparkSession.builder.appName('reduce').getOrCreate()

# Read the file
lines = sc.sparkContext.textFile("number.txt")

# Process the data
numbers = lines.flatMap(lambda line: line.split("\t"))
valid_numbers = numbers.filter(lambda number: number)
int_numbers = valid_numbers.map(lambda number: int(number))

# Calculate and print the sum
print("Sum is: {}".format(int_numbers.reduce(lambda x, y: x + y)))
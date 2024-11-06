import pyspark
from pyspark.sql import SparkSession
sc=SparkSession.builder.appName('reduce').getOrCreate()
lines=sc.textFile("number.txt")
numbers=lines.flatmap(lambda line:line.split("\t"))
validnumber=numbers.filter(lambda number:  number)
intnumber=validnumber.map(lambda number: int(number))
print("sum is: {}".format(intnumber.reduce(lambda x,y :x+y)))

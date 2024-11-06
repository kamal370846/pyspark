from pyspark.sql import SparkSession
spark=SparkSession.builder.getOrCreate()
# df=spark.createDataFrame([
#     (1,"ram",2345,"dev"),
#     (3,"shyam",5647,"dev"),
#     (2,"roy",2308,"tester"),
#     (5,"bhim",1235,"support")
# ],
# schema='id int,name string,sal int,desig string')
# df.show()
from pyspark.sql.functions import upper
import pandas as pd
df1=pd.DataFrame({
    'id':(1,2,3,4,5),
    'name':("ram","shyam","raghu","brinda","ashok"),
    'sal':(123,234,766,423,345),
    'desig':("dev","test","test","support","dev")
})
df1_a=spark.createDataFrame(df1)
df1_a.show()
df1_a.printSchema()
df1_a.show(1)
df1_a.select("id", "name", "sal", "desig").describe().show()
df1_a.collect()
df1_a.take(1)
df1_a.id
df1_a.select(upper(df1_a.name)).show()
df1_a.select('name',upper(df1_a.name)).show()
df1_a.withColumn('case23',upper(df1_a.name)).show()
df1_a.filter(df1_a.id==1).show()
df1_a.select(df1_a.name).show()
df1_a.select("name").show()

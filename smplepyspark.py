from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("test").getOrCreate()
df=spark.createDataFrame([(1, "Ramesh",5000,300), (2, "Suresh",4000,800)], ["id", "name","salary","bonus"])
df.show()

@udf(returnType=IntegerType())
# @udf('IntegerType')
def totalpay(s,b):
    return s+b

#Totalpayment=udf(lambda s,b:totalpay(s,b), IntegerType())
#df.withColumn('totpay',Totalpayment(df.salary,df.bonus)).show()

df.select('*', totalpay(df.salary,df.bonus).alias('TotalSal')).show()


import pyarrow
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf('long')
def pandas_plus_one(series: pd.Series) -> pd.Series:
    # Simply plus one by using pandas Series.
    return series + 1

df.select(pandas_plus_one(df.id)).show()
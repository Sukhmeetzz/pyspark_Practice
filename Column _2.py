from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
spark = SparkSession.builder.appName('From Struct ').master('local[3]').getOrCreate()
data = [Row(name='james', prop=Row(hair='black', eyes='blue')),
        Row(name='peter', prop=Row(hair='red', eyes='black'))]
df = spark.createDataFrame(data)
df.printSchema()

# How to access columns
df.select(df.prop.hair).show()
df.select('prop.hair').show()
df.select(col('prop.hair')).show()

# accessing all columns
df.select(col('prop.*')).show()
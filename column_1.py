# Creating a column class

# starting with Session
from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import col

spark = SparkSession.builder.master('local[*]').getOrCreate()
data = [("James", 23), ("Ann", 40)]
df = spark.createDataFrame(data).toDF("name", "gender")
df.select(col("gender")).show()
df.select(col("name")).show()

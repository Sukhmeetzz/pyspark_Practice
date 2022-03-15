

# creating DataFrame
# 1) Starting SparkSession
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[*]').getOrCreate()

# 2)  creating DataFrame from RDD
data = [('python', 2000), ('scala', 3000), ('sql', 1500), ('java',5000)]
cols = ['language', 'user']

# creating RDD
rdd = spark.sparkContext.parallelize(data)

# now creating DAtaFRame from RDD

Df = rdd.toDF()

# To create DataFrame with column name we need to add cols as arguments in toDF() method

Df = rdd.toDF(cols)
Df.show()
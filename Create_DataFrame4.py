# Creating DataFrame From CSV File

from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[*]').appName('Creating DF from CSV file').getOrCreate()
df = spark.read.option('header', True).csv('/home/oem/Downloads/Sample_data.csv')
# If you have a header with column names on your input file then use [.option('header', True)] as shown above
df.printSchema()
df.show()
# The syntax of .show() method is :
# .show(self, n=20, truncate=True, vertical=False)


from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[*]').appName('Creating DF from CSV file').getOrCreate()
df = spark.read.option('header', True).csv('/home/oem/Downloads/Sample_data.csv')
# for displaying only 4 rows n should be equal to 4
df.show(4, truncate=False)

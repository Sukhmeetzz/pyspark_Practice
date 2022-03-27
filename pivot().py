# PySpark SQL provides pivot() function to rotate the data from one column into multiple columns. It is an aggregation
# where one of the grouping columns values is transposed into individual columns with distinct data.

from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[*]').appName('pivot()').getOrCreate()

data = [("Banana", 1000, "USA"), ("Carrots", 1500, "USA"), ("Beans", 1600, "USA"),
        ("Orange", 2000, "USA"), ("Orange", 2000, "USA"), ("Banana", 400, "China"),
        ("Carrots", 1200, "China"), ("Beans", 1500, "China"), ("Orange", 4000, "China"),
        ("Banana", 2000, "Canada"), ("Carrots", 2000, "Canada"), ("Beans", 2000, "Mexico")]

columns = ["Product", "Amount", "Country"]
df = spark.createDataFrame(data=data, schema=columns)
pivotDF = df.groupby('country').pivot('product').sum('amount')
pivotDF.show()

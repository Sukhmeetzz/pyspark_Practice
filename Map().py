# PySpark map (map()) is an RDD transformation that is used to apply the transformation function (lambda) on every
# element of RDD/DataFrame and returns a new RDD.


# RDD map() transformation is used to apply any complex operations like adding a column, updating a column,
# transforming the data e.t.c, the output of map transformations would always have the same number of records as input.

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]") \
    .appName("SparkByExamples.com").getOrCreate()

data = ["Project", "Gutenberg’s", "Alice’s", "Adventures",
        "in", "Wonderland", "Project", "Gutenberg’s", "Adventures",
        "in", "Wonderland", "Project", "Gutenberg’s"]

rdd = spark.sparkContext.parallelize(data)

# PySpark map() Example with RDD

rdd2 = rdd.map(lambda x: (x, 1))
# for element in rdd2.collect():
#     print(element)
print(rdd2.collect())

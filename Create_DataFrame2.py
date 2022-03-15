#  We can create DataFrame by CReateDataFrame method

# starting SparkSession
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[*]').appName('createDataFrame').getOrCreate()

# again we use the same data as:

data = [('python', 2000), ('scala', 3000), ('sql', 1500), ('java', 5000)]
cols = ['language', 'user']

# creating RDD
rdd = spark.sparkContext.parallelize(data)

# creating DAtaFrame by CreateDataFrame() method and using rdd as an argument.again we we will chain .toDF() and add
# cols as an argument to add columns

# DF = spark.createDataFrame(rdd).toDF(*cols)
# DF.show()

# also i can use list as an agrument to create DataFrame

DF2 = spark.createDataFrame(data).toDF(*cols)
DF2.show()

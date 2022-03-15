# Using Row class on pyspark DataFrame

from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = [Row(name="James,,Smith", lang=["Java", "Scala", "C++"], state="CA"),
        Row(name="Michael,Rose,", lang=["Spark", "Java", "C++"], state="NJ"),
        Row(name="Robert,,Williams", lang=["CSharp", "VB"], state="NV")]

df = spark.createDataFrame(data)
df.printSchema()
df.show()

# You can also change the column names by using toDF() function

columns = ['name', 'lang_at_school', 'current_state']
df2 = spark.createDataFrame(data).toDF(*columns)
df2.show()

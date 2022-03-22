from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.master('local[*]').appName('select_2').getOrCreate()
data = [
    (("James", None, "Smith"), "OH", "M"),
    (("Anna", "Rose", ""), "NY", "F"),
    (("Julia", "", "Williams"), "OH", "F"),
    (("Maria", "Anne", "Jones"), "NY", "M"),
    (("Jen", "Mary", "Brown"), "NY", "M"),
    (("Mike", "Mary", "Williams"), "OH", "M")
]
schema = StructType([
    StructField('name', StructType([
        StructField('first_name', StringType(), True),
        StructField('middle_name', StringType(), True),
        StructField('last_name', StringType(), True)
    ])),
    StructField('State', StringType(), True),
    StructField('gender', StringType(), True)
])
df = spark.createDataFrame(data=data, schema=schema)
df.printSchema()
df.show(truncate=False)
df.select(df.name).show(truncate=False)
df.select('name.first_name', 'name.last_name').show()
df.select('name.*').show()
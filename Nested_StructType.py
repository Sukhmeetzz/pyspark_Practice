from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.master('local[*]').appName('Nested_StructType').getOrCreate()
structureData = [
    (("James", "", "Smith"), "36636", "M", 3100),
    (("Michael", "Rose", ""), "40288", "M", 4300),
    (("Robert", "", "Williams"), "42114", "M", 1400),
    (("Maria", "Anne", "Jones"), "39192", "F", 5500),
    (("Jen", "Mary", "Brown"), "", "F", -1)
]

# structureSchema = StructType([
#     StructField('name', StructType([
#         StructField('first_name', StringType(), True),
#         StructField('middle_name',StringType(), True),
#         StructField('last_name', StringType(), True)
#     ])),
#     StructField('id', StringType(), True),
#     StructField('gender', StringType(), True),
#     StructField('salary', IntegerType(), True)
# ])
structureSchema = StructType([
    StructField('name', StructType([
        StructField('first_name', StringType(), True),
        StructField('middle_name', StringType(), True),
        StructField('last_name', StringType(), True)
    ])),
    StructField('id', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('salary', IntegerType(), True)
])
df = spark.createDataFrame(data=structureData, schema=structureSchema)
df.printSchema()
df.show(truncate=False)
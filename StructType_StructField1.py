# StructType is a collection of StructField’s that defines column name, column data type, boolean to specify
#  if the field can be nullable or not and metadata.
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.master('local[*]').appName('SructType and StructField').getOrCreate()

data = [("James", "", "Smith", "36636", "M", 3000),
        ("Michael", "Rose", "", "40288", "M", 4000),
        ("Robert", "", "Williams", "42114", "M", 4000),
        ("Maria", "Anne", "Jones", "39192", "F", 4000),
        ("Jen", "Mary", "Brown", "", "F", -1)
        ]

schema = StructType([
    StructField('first_name', StringType(), True),
    StructField('middle_name', StringType(), True),
    StructField('last_name', StringType(), True),
    StructField('id', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('salary', IntegerType(), True)])
df = spark.createDataFrame(data=data, schema=schema)
df.printSchema()
df.show(truncate=False)
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, ArrayType

spark = SparkSession.builder.master('local[*]').appName('where() and filter()').getOrCreate()

data = [
    (("James", "", "Smith"), ["Java", "Scala", "C++"], "OH", "M"),
    (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
    (("Julia", "", "Williams"), ["CSharp", "VB"], "OH", "F"),
    (("Maria", "Anne", "Jones"), ["CSharp", "VB"], "NY", "M"),
    (("Jen", "Mary", "Brown"), ["CSharp", "VB"], "NY", "M"),
    (("Mike", "Mary", "Williams"), ["Python", "VB"], "OH", "M")
]

schema = StructType([
    StructField('name', StructType([
        StructField('fname', StringType(), True),
        StructField('mname', StringType(), True),
        StructField('lname', StringType(), True)
    ])),
    StructField('language', ArrayType(StringType()), True),
    StructField('state', StringType(), True),
    StructField('gender', StringType(), True)
])
df = spark.createDataFrame(data=data, schema=schema)
df.printSchema()
df.show()

# DataFrame filter() with Column Condition

df.filter(df.state == 'OH').show()
df.filter(df.state != 'OH').show()
df.filter(df.gender == 'M').show()
df.filter(df.gender != 'M').show()

# PySpark Filter with Multiple Conditions

df.filter((df.state == 'OH') & (df.gender == 'M')).show()

# Filter Based on List Values

li = ["OH", "CA", "DE"]
df.filter(df.state.isin(li)).show()


# Filter Based on Starts With, Ends With, Contains
df.filter(df.state.startswith('N')).show()
df.filter(df.state.endswith('H')).show()
df.filter(df.state.contains('H')).show()

#  Filtering on Nested Struct columns

df.filter(df.name.lname == 'William').show()
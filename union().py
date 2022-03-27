# PySpark union() and unionAll() transformations are used to merge two or more DataFrame’s of the
# same schema or structure.

from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[*]').appName('union()').getOrCreate()

# here is out first dataset
simpleData = [("James", "Sales", "NY", 90000, 34, 10000),
              ("Michael", "Sales", "NY", 86000, 56, 20000),
              ("Robert", "Sales", "CA", 81000, 30, 23000),
              ("Maria", "Finance", "CA", 90000, 24, 23000)
              ]

columns = ["employee_name", "department", "state", "salary", "age", "bonus"]
df = spark.createDataFrame(data=simpleData, schema=columns)

# here is our second dataset

simpleData2 = [("James", "Sales", "NY", 90000, 34, 10000),
               ("Maria", "Finance", "CA", 90000, 24, 23000),
               ("Jen", "Finance", "NY", 79000, 53, 15000),
               ("Jeff", "Marketing", "CA", 80000, 25, 18000),
               ("Kumar", "Marketing", "NY", 91000, 50, 21000)
               ]
columns2 = ["employee_name", "department", "state", "salary", "age", "bonus"]

df2 = spark.createDataFrame(data=simpleData2, schema=columns2)

# Merge two or more DataFrames using union

# unionDF = df.union(df2)
# unionDF.show(truncate=False)

# Merge without Duplicates

unionDF = df.union(df2).distinct()
unionDF.show(truncate=False)
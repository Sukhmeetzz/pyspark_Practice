from pyspark.sql import SparkSession


spark = SparkSession.builder.master('local[*]').appName('GroupBy()').getOrCreate()

simpleData = [("James", "Sales", "NY", 90000, 34, 10000),
              ("Michael", "Sales", "NY", 86000, 56, 20000),
              ("Robert", "Sales", "CA", 81000, 30, 23000),
              ("Maria", "Finance", "CA", 90000, 24, 23000),
              ("Raman", "Finance", "CA", 99000, 40, 24000),
              ("Scott", "Finance", "NY", 83000, 36, 19000),
              ("Jen", "Finance", "NY", 79000, 53, 15000),
              ("Jeff", "Marketing", "CA", 80000, 25, 18000),
              ("Kumar", "Marketing", "NY", 91000, 50, 21000)
              ]

schema = ["employee_name", "department", "state", "salary", "age", "bonus"]
df = spark.createDataFrame(data=simpleData, schema=schema)
df.printSchema()
df.show(truncate=False)


# PySpark groupBy and aggregate on multiple columns
# df.groupby('department', 'state').sum('salary', 'bonus').show()

# Running more aggregates at a time


# Using agg() aggregate function we can calculate many aggregations at a time on a single statement using PySpark SQL
# aggregate functions sum(), avg(), min(), max() mean() e.t.c
print('calculating many aggregate functions ')
df.groupBy("department") \
    .agg(sum("salary").alias("sum_salary"),
         avg("salary").alias("avg_salary"),
         sum("bonus").alias("sum_bonus"),
         max("bonus").alias("max_bonus")
         ) \
    .show(truncate=False)
# PySpark map() Example with DataFrame

# PySpark DataFrame doesn’t have map() transformation to apply the lambda function, when you wanted to apply the custom
# transformation, you need to convert the DataFrame to RDD and apply the map() transformation

from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[*]').appName('Map with DataFrame').getOrCreate()
data = [('James', 'Smith', 'M', 30),
        ('Anna', 'Rose', 'F', 41),
        ('Robert', 'Williams', 'M', 62),
        ]

columns = ["firstname", "lastname", "gender", "salary"]
df = spark.createDataFrame(data=data, schema=columns)
df.show()

# # Refering columns by index.
# rdd = df.rdd.map(lambda x: (x[0] + ',' + x[1], x[2], x[3] * 2))
# df2 = rdd.toDF(['name', 'gender', 'new_salary'])
# df2.show()

# Referring Column Names

# rdd = df.rdd.map(lambda x: (x.firstname + ',' + x.lastname, x.gender, x.new_salary * 2))
# df2 = rdd.toDF(['name', 'gender', 'new_salary'])
# df2.show()


#  By Calling function

def func1(x):
    firstname = x.firstname
    lastname = x.lastname
    name = x.firstname + ',' + x.lastname
    gender = x.gender
    salary = x.salary*2
    return (name, gender, salary)

rdd2=df.rdd.map(lambda x: func1(x))
df2 = rdd2.toDF(['name', 'gender', 'salary'])
df2.show()
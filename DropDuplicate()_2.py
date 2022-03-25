from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[*]').appName('distinct()/DropDuplicate()').getOrCreate()
data = [("James", "Sales", 3000),
        ("Michael", "Sales", 4600),
        ("Robert", "Sales", 4100),
        ("Maria", "Finance", 3000),
        ("James", "Sales", 3000),
        ("Scott", "Finance", 3300),
        ("Jen", "Finance", 3900),
        ("Jeff", "Marketing", 3000),
        ("Kumar", "Marketing", 2000),
        ("Saif", "Sales", 4100)
        ]
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=data, schema=columns)
df.printSchema()
print('original count ' + str(df.count()))
df.show(truncate=False)

# PySpark Distinct of Selected Multiple Columns

df2 = df.dropDuplicates(['employee_name', 'department', 'salary'])
print('dropDuplicate count: ' + str(df2.count()))
df2.show(truncate=False)
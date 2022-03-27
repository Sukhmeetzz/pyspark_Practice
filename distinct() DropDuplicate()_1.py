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


# Get Distinct Rows (By Comparing All Columns)
# distinctDF = df.distinct()
# print('Distinct count: ' + str(distinctDF.count()))
# distinctDF.show(truncate=False

# Alternatively, you can also run dropDuplicates() function which returns a new DataFrame after removing duplicate rows.

df2 = df.dropDuplicates()
print('DropDuplicate count: ' + str(df2.count()))

df2.show()

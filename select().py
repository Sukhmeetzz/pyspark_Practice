from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[*]').appName('select()').getOrCreate()

data = [("James", "Smith", "USA", "CA"),
        ("Michael", "Rose", "USA", "NY"),
        ("Robert", "Williams", "USA", "CA"),
        ("Maria", "Jones", "USA", "FL")
        ]
columns = ["firstname", "lastname", "country", "state"]
df = spark.createDataFrame(data=data, schema=columns)
df.show(truncate=False)

# Select Single & Multiple Columns From PySpark
df.select(df.firstname, df.lastname).show()

# 2. Select All Columns From List
df.select(*columns).show()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.master('local[*]').appName('ChangeColumnDataType').getOrCreate()

# PySpark withColumn – To change column DataType
# Transform/change value of an existing column
# Derive new column from an existing column
# Add a column with the literal value
# Rename column name
# Drop DataFrame column

data = [('James', '', 'Smith', '1991-04-01', 'M', 3000),
        ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
        ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
        ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
        ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)
        ]

columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
df = spark.createDataFrame(data=data, schema=columns)
df.printSchema()

# Change DataType using PySpark withColumn()
# df.withColumn('salary', col('salary').cast('Integer')).show()
df.withColumn('salary', df.salary.cast('Integer')).show()
df.withColumn('firstname', df.firstname.cast('Integer')).printSchema()
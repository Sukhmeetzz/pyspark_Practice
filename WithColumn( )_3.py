from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.master('local[*]').appName('Create Column From Existing').getOrCreate()

data = [('James', '', 'Smith', '1991-04-01', 'M', 3000),
        ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
        ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
        ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
        ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)
        ]

columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
df = spark.createDataFrame(data=data, schema=columns)
# df.show()
# Create a Column from an Existing
df.withColumn('copied_col', col('salary')*-1).show()
df.withColumn('copied_col', col('salary')).show()
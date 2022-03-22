from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.master('local[*]').appName('Add New Column').getOrCreate()

data = [('James', '', 'Smith', '1991-04-01', 'M', 3000),
        ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
        ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
        ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
        ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)
        ]

columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
df = spark.createDataFrame(data=data, schema=columns)
df.show()
df.withColumn('Country', lit('USA')) \
        .withColumn('another_col', lit('anotherValue')).show()

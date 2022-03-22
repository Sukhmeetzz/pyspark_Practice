from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import when

spark = SparkSession.builder.master('local[*]').appName('column functions').getOrCreate()
data = [("James", "Bond", "100", None),
        ("Ann", "Varsa", "200", 'F'),
        ("Tom Cruise", "XXX", "400", ''),
        ("Tom Brand", None, "400", 'M')]
columns = ["fname", "lname", "id", "gender"]
df: DataFrame = spark.createDataFrame(data, columns)
# when() & otherwise() – It is similar to SQL Case When, executes sequence of expressions until it matches the
# condition and returns a value when match.

df.select(df.fname, df.lname, when(df.gender == 'M', 'Male')
          .when(df.gender == 'F', 'FEMALE')
          .when(df.gender == "None", '')
          .otherwise(df.gender).alias('new gender')).show()


from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[*]').appName('asc() desc()').getOrCreate()
data = [("James", "Bond", "100", None),
        ("Ann", "Varsa", "200", 'F'),
        ("Tom Cruise", "XXX", "400", ''),
        ("Tom Brand", None, "400", 'M')]
columns = ["fname", "lname", "id", "gender"]
df = spark.createDataFrame(data, columns)
df.sort(df.fname.asc()).show()
df.sort(df.lname.desc()).show()

# cast() & astype() Used to convert the data Type.
df.select(df.fname, df.id.cast('int')).printSchema()

# between() – Returns a Boolean expression when a column values in between lower and upper bound.
df.filter(df.id.between(100, 300)).show()

# contains() – Checks if a DataFrame column value contains a a value specified in this function.
df.filter(df.fname.contains("Cruise")).show()

# startswith() & endswith() – Checks if the value of the DataFrame Column starts and ends with a String respectively.

df.filter(df.fname.startswith('T')).show()
df.filter(df.fname.endswith('Cruise')).show()

#  isNull & isNotNull() – Checks if the DataFrame column has NULL or non NULL values.

df.filter(df.lname.isNull()).show()
df.filter(df.lname.isNotNull()).show()
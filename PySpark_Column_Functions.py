from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession.builder.master('local[*]').appName('alias').getOrCreate()
data = [("James", "Bond", "100", None),
        ("Ann", "Varsa", "200", 'F'),
        ("Tom Cruise", "XXX", "400", ''),
        ("Tom Brand", None, "400", 'M')]
columns = ['fname', 'lname', 'id', 'gender']
df = spark.createDataFrame(data).toDF(*columns)
# df2 = spark.createDataFrame(data=data, schema=columns)
# df3 = spark.createDataFrame(data, columns)
# df.show()
# df2.show()
# df3.show()
df.select(df.fname.alias('first_name'),
          df.lname.alias('last_name'),
          df.id.alias('identity_no'),
          df.gender).show()




from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, explode_outer, posexplode, posexplode_outer


spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

arrayData = [
    ('James', ['Java', 'Scala'], {'hair': 'black', 'eye': 'brown'}),
    ('Michael', ['Spark', 'Java', None], {'hair': 'brown', 'eye': None}),
    ('Robert', ['CSharp', ''], {'hair': 'red', 'eye': ''}),
    ('Washington', None, None),
    ('Jefferson', ['1', '2'], {})]
df = spark.createDataFrame(data=arrayData, schema=['name', 'knownLanguages', 'properties'])


# PySpark function explode(e: Column) is used to explode or create array or map columns to rows. When an array is
# passed to this function, it creates a new default column “col1” and it contains all array elements. When a map is
# passed, it creates two new columns one for key and one for value and each element in map split into the rows.
# This will ignore elements that have null or empty
# This will ignore elements that have null or empty. Since the Washington and Jefferson have null or empty values in
# array and map, the following snippet out does not contain these.

# when array is passed
df2 = df.select(df.name, explode(df.knownLanguages))
df2.printSchema()
df2.show()
# when map is passed
df2 = df.select(df.name, explode((df.properties)))
df2.show()


# explode_outer function is used to create a row for each element in the array or map column. Unlike explode,
# if the array or map is null or empty, explode_outer returns null.

#  when map is passed
df2 = df.select(df.name, explode_outer(df.properties))
df2.show()

# when array is passed

df2 = df.select(df.name, explode_outer(df.knownLanguages))
df2.show()


# creates a row for each element in the array and creates two columns “pos’ to hold the position of the array element
# and the ‘col’ to hold the actual array value. And when the input column is a map, posexplode function creates 3
# columns “pos” to hold the position of the map element, “key” and “value” columns.


# when array is passed
df2 = df.select(df.name, posexplode(df.knownLanguages)).show()

# when mapi is passed
df2 = df.select(df.name, posexplode(df.properties)).show()


# posexplode_outer creates a row for each element in the array and creates two columns “pos’ to hold the position
# of the array element and the ‘col’ to hold the actual array value. Unlike posexplode, if the array or map is null
# or empty, posexplode_outer function returns null, null for pos and col columns. Similarly for the map, it returns
# rows with nulls.

# when array is passed
df2 = df.select(df.name, posexplode_outer(df.knownLanguages)).show()

# when map is passed

df2 = df.select(df.name, posexplode_outer(df.properties)).show()
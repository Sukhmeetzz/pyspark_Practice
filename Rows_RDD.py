# Create a row object
from pyspark.sql import SparkSession, Row

row = Row(name='Sukhmeet', age=29, gender='male')
print(row.name, row.age, row.gender)

# Create Custom Class from Row
person = Row('name', 'age')
p1 = person('sam', 32)
p2 = person('tom', 36)
print(p1.name + ',' + p2.name)

#  Using Row class on PySpark RDD


spark = SparkSession.builder.master("local[*]").appName('Create RDD From Row').getOrCreate()
data = [Row(name="James,,Smith", lang=["Java", "Scala", "C++"], state="CA"),
        Row(name="Michael,Rose,", lang=["Spark", "Java", "C++"], state="NJ"),
        Row(name="Robert,,Williams", lang=["CSharp", "VB"], state="NV")]
rdd = spark.sparkContext.parallelize(data)
print(rdd.collect())

# Now, let’s collect the data and access the data using its properties.
collData = rdd.collect()
for row in collData:
    print(row.name + ',' + str(row.lang))

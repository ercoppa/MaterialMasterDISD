from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, column, expr
from pyspark.sql.types import *
from pyspark.sql.functions import *

# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html#pyspark.sql.DataFrame

spark = SparkSession.builder.appName('DISD').master('local[*]').getOrCreate()

print()

records = [(0, 'Edoardo', 36, 'Roma'), (1, 'Ginevra', 16, 'Firenze'), (2, 'Simona', 53, 'Napoli'), (3, 'Leonardo', 50, 'Napoli')]

field_id = StructField("id", LongType(), nullable=False)
field_name = StructField("nome", StringType(), nullable=False)
field_year = StructField("eta", ByteType())
field_place = StructField("luogo di nascita", StringType())

schema = StructType([field_id, field_name, field_year, field_place])

df = spark.createDataFrame(records, schema)  
df.printSchema()

print()

# select

df.select("*").show()
df.select("luogo di nascita", "eta").show()
df.select(col("luogo di nascita").alias('luogo'), expr("eta")).show()
df.select(expr("`luogo di nascita` AS luogo"), "eta").show()
df = df.withColumnRenamed('luogo di nascita', 'luogo')
df.show()

print()

# selectExpr

df.select(expr("luogo").alias('Luogo'), expr("eta AS Eta")).show()
df.selectExpr("luogo AS Luogo", "eta AS Eta").show()

print()

# where/filter
df.selectExpr("luogo", "eta").filter("eta > 20").show()
df.selectExpr("luogo", "eta").where("eta > 20").show()
df.selectExpr("luogo", "eta").where(col("eta") > 20).show()
df.selectExpr("luogo", "eta").where(col("eta") > lit(20)).show()

print()

# aggregate functions

df.groupBy('luogo').agg(count('luogo').alias("count")).show()
df.groupBy('luogo').agg(expr("count(luogo) AS count")).show()
df.selectExpr('luogo', 'eta', 'id').groupBy('luogo').agg({'id':'avg', 'eta': 'count'}).show()
df.groupBy('luogo').count().show()
df.groupBy('luogo').avg().show()

print()

# join

l1 = [('a', 1), ('b', 2), ('c', 3)]
l2 = [('b', 4), ('b', 5), ('d', 6)]

df1 = spark.createDataFrame(l1)
df2 = spark.createDataFrame(l2)

df1.show()
df2.show()

df1.join(df2, on=df1['_1'] == df2['_1']).select(df1['_1'].alias('df1_1'), df1['_2'].alias('df1_2'), df2['_1'].alias('df2_1'), df2['_2'].alias('df2_2')).show()
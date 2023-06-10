from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, column, expr
from pyspark.sql.types import *

spark = SparkSession.builder.appName('DISD').master('local[*]').getOrCreate()

df = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load('data/prova.csv')

df.printSchema()
df.show()

print()

# inferSchema is False by default and struct types are set to String
df = spark.read.format('csv').option("header", "true").load('data/prova.csv').printSchema()

# explicit set the schema

field_id = StructField("id", LongType(), nullable=False)
field_name = StructField("nome", StringType(), nullable=False)
field_year = StructField("eta", ByteType())
field_place = StructField("luogo di nascita", StringType())

schema = StructType([field_id, field_name, field_year, field_place])

df = spark.read.schema(schema).format('csv').option("header", "true").load('data/prova.csv').printSchema()

print()

# shortcut for CSV files
df = spark.read.csv('data/prova.csv', header=True, inferSchema=True)

df.printSchema()
df.show()
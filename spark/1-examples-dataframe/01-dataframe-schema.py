from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, column, expr
from pyspark.sql.types import *

spark = SparkSession.builder.appName('DISD').master('local[*]').getOrCreate()

print()

records = [(0, 'Edoardo', 36, 'Roma'), (1, 'Ginevra', 16, 'Firenze'), (2, 'Simona', 53, 'Napoli'), (3, 'Leonardo', 50, 'Napoli')]

# dataframe is created and schema is inferred by Spark
df = spark.createDataFrame(records)  
df.printSchema()

print()

# schema is inferred by Spark but we choose column names
df = spark.createDataFrame(records, ['id', 'nome', 'eta', 'luogo di nascita'])  
df.printSchema()

print()

# we explicit set the schema

field_id = StructField("id", LongType(), nullable=False)
field_name = StructField("nome", StringType(), nullable=False)
field_year = StructField("eta", ByteType())
field_place = StructField("luogo di nascita", StringType())

schema = StructType([field_id, field_name, field_year, field_place])

df = spark.createDataFrame(records, schema)  
df.printSchema()

print()

# show dataframe content

df.show()

print()

# access columns from dataframe

print(df.columns)
print(df.id)
print(df['id'])

print()

# print subset of dataframe wrt some columns
df[['nome', 'id']].show()
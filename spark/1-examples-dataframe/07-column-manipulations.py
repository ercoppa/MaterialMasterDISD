from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, column, expr
from pyspark.sql.types import *
from pyspark.sql.functions import lit

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

# add column
df.withColumn("2eta", expr("eta * 2")).show()

print()

# add column with constant value
df.withColumn('esame', lit("passato")).show()

print()

# update column
df.withColumn("eta", expr("eta - 6")).show()

print()

# rename column
df.withColumnRenamed("luogo di nascita", "luogo").show()

# drop a column
df.drop("id").show()
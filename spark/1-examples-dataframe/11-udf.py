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

def add_gender(nome):
  if nome[-1] == 'o':
    return "maschio"
  else:
    return "femmina"

add_gender_udf = udf(add_gender)

df.withColumn('sesso', add_gender_udf("nome")).show()
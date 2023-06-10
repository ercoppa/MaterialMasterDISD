from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, column, expr
from pyspark.sql.types import *

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

df.write.mode("overwrite").format('csv').option("header", "true").save('output')
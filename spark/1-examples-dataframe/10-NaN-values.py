from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, column, expr
from pyspark.sql.types import *
from pyspark.sql.functions import *

# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html#pyspark.sql.DataFrame

spark = SparkSession.builder.appName('DISD').master('local[*]').getOrCreate()

print()

records = [('a', 1, 'b'), (None, 2, 'c'), ('c', None, 'a')]

df_with_missing = spark.createDataFrame(records)
df_with_missing.show()

df_with_missing_number = df_with_missing.fillna('b', subset=['_1', '_3'])
df_with_missing_number.show()

df_with_missing_number.dropna(subset=['_2']).show()


records = [('a', 6), ('b', None), ('c', 2)]

df_with_missing = spark.createDataFrame(records)
df_with_missing.show()

avg_value = df_with_missing.select(avg(col('_2'))).collect()[0][0]
df_with_missing.fillna(avg_value, subset=['_2']).show()
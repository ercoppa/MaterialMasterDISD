from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, column, expr
from pyspark.sql.types import *

spark = SparkSession.builder.appName('DISD').master('local[*]').getOrCreate()

print()

df = spark.read.text(
    'data/prova.txt',   # path
    lineSep="\n",       # default
)

df.printSchema()
df.show()
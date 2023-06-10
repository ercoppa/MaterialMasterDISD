from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, column, expr
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('DISD').master('local[*]').getOrCreate()

print()

lines = spark.read.text('input')
lines.createOrReplaceTempView('lines_view')

top = spark.sql("SELECT word, COUNT(word) AS count FROM (SELECT explode(split(value, ' ')) AS word FROM lines_view) GROUP BY word ORDER BY count DESC LIMIT 5")

if True:
    print(top)

print(top.collect())
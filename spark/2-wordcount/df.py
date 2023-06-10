from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('DISD').master('local[*]').getOrCreate()

print()

lines = spark.read.text('input')
words = lines.select(explode(split(lines.value, " ")).alias("word"))
words = words.groupBy('word').count()
top = words.orderBy('count', ascending=False).select('word', 'count').limit(5).collect()

if False:
    print(lines)
    lines.show()
    print(words)
    words.show()

print(top)
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window

spark = SparkSession.builder.appName('DISD').master('local[*]').getOrCreate()

print()

artist = StructField("artist", StringType(), nullable=True)
duration = StructField("duration", FloatType(), nullable=True)
hotness = StructField("hotness", FloatType(), nullable=True)
title = StructField("title", StringType(), nullable=True)
year = StructField("year", LongType(), nullable=True)
schema = StructType([artist, duration, hotness, title, year])

lines = spark.read.schema(schema).csv('input/')
lines = lines.dropna(subset=['hotness', 'year', 'title'])
lines = lines.withColumn('decade', expr('floor(year/10)'))
lines = lines.select("*").where('decade > 0')
lines = lines.distinct()

w = Window.partitionBy('decade')

lines = lines.withColumn('max', max('hotness').over(w)).where('max == hotness').drop('max')

if False:
    print(lines)
    lines.show()

print(lines.collect())
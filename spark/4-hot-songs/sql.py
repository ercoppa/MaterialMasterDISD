from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, column, expr
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('DISD').master('local[*]').getOrCreate()

print()

artist = StructField("artist", StringType(), nullable=True)
duration = StructField("duration", FloatType(), nullable=True)
hotness = StructField("hotness", FloatType(), nullable=True)
title = StructField("title", StringType(), nullable=True)
year = StructField("year", LongType(), nullable=True)
schema = StructType([artist, duration, hotness, title, year])

lines = spark.read.schema(schema).csv('input')

lines.createOrReplaceTempView('lines_view')

top = spark.sql("SELECT * FROM (SELECT *, MAX(hotness) OVER (PARTITION BY decade) AS max FROM (SELECT DISTINCT floor(year/10) AS decade, artist, duration, hotness, title, year FROM lines_view WHERE title IS NOT NULL AND artist IS NOT NULL AND year > 0 AND hotness > 0)) WHERE hotness = max")

if False:
    print(top)

print(top.collect())
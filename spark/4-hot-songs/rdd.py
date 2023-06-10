from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('DISD').master('local[*]').getOrCreate()

print()

lines = spark.sparkContext.textFile('input/')
lines = lines.map(lambda x: x.split(","))
lines = lines.filter(lambda x: x[2] != 'nan' and x[3] != 'nan' and x[4] != 'nan' and x[4] != '0')
lines = lines.map(lambda x: (x[4][0:-1], (x[0], x[1], float(x[2]), x[3], x[4])))
lines = lines.reduceByKey(lambda x, y: x if x[2] > y[2] else y)
lines = lines.sortByKey().map(lambda x: x[1])
top_songs = lines.collect()

print(top_songs)

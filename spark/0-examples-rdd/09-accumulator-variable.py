from pyspark.sql import SparkSession
from random import randint

spark = SparkSession.builder.appName('DISD').master('local[*]').getOrCreate()

words = "tre tigri contro tre tigri".split(" ")
map_random = {w: randint(0, 1000) for w in words}

print("\nPython data:\n\t%s\n\ttype=%s\n" % (words, type(words)))

words_rdd = spark.sparkContext.parallelize(words, 8)
map_random_broad = spark.sparkContext.broadcast(map_random)

def map_random_f(w):
  return w, map_random_broad.value[w]

data = words_rdd.map(map_random_f).collect()

print("\nPython data after map:\n\t%s\n\ttype=%s\n" % (data, type(data)))

map_random_broad.unpersist()  # delete the broadcast variable


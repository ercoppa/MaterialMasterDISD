from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel
from random import randint

spark = SparkSession.builder.appName('DISD').master('local[*]').getOrCreate()

words = "tre tigri contro tre tigri".split(" ")

print("\nPython data:\n\t%s\n\ttype=%s\n" % (words, type(words)))

words_rdd = spark.sparkContext.parallelize(words, 8)

words_rdd.persist(StorageLevel.MEMORY_ONLY)
print(words_rdd.toDebugString())
print(words_rdd.count())
print(words_rdd.take(3))
print(words_rdd.toDebugString())
words_rdd.unpersist()

print()

words_rdd.persist(StorageLevel.DISK_ONLY)
print(words_rdd.count())
print(words_rdd.take(3))
print(words_rdd.toDebugString())
words_rdd.unpersist()
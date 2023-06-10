from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('DISD').master('local[*]').getOrCreate()

print()

lines = spark.sparkContext.textFile('input/')
words = lines.flatMap(lambda x: x.split(" "))
words = words.map(lambda x: (x, 1))
words = words.reduceByKey(lambda x, y: x + y)
top = words.top(5, key=lambda x: x[1])

if False: # debug only
    print()
    print(words.collect())
    print()

print(top)

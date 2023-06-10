from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('DISD').master('local[*]').getOrCreate()

words = "tre tigri contro tre tigri".split(" ")

print("\nPython data:\n\t%s\n\ttype=%s\n" % (words, type(words)))

words_rdd = spark.sparkContext.parallelize(words, 8)

print("\nSpark RDD:\n\tobject=%s\n\ttype=%s\n\tnumPartitions=%d\n" % (words_rdd, type(words_rdd), words_rdd.getNumPartitions()))

# collect
data = words_rdd.collect()

print("\nPython data after transformation collect:\n\t%s\n\ttype=%s\n" % (data, type(data)))

# reduce
data = words_rdd.map(lambda w: 1).reduce(lambda v1, v2: v1 + v2)

print("\nPython data after transformation reduce:\n\t%s\n\ttype=%s\n" % (data, type(data)))

# count
data = words_rdd.filter(lambda w: w != "tre").distinct().count()

print("\nPython data after transformation count:\n\t%s\n\ttype=%s\n" % (data, type(data)))

# aggregate
data = words_rdd.aggregate(
    0,                          # zero value
    lambda tot, v: tot + 1,     # merge within partition
    lambda v1, v2: v1 + v2      # merge across partitions
)

print("\nPython data after transformation aggregate:\n\t%s\n\ttype=%s\n" % (data, type(data)))

# take
data = words_rdd.map(lambda w: (w, 1)).sortByKey().take(1)

print("\nPython data after transformation take:\n\t%s\n\ttype=%s\n" % (data, type(data)))
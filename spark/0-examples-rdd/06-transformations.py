from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('DISD').master('local[*]').getOrCreate()

words = "tre tigri contro tre tigri".split(" ")

print("\nPython data:\n\t%s\n\ttype=%s\n" % (words, type(words)))

words_rdd = spark.sparkContext.parallelize(words, 8)

print("\nSpark RDD:\n\tobject=%s\n\ttype=%s\n\tnumPartitions=%d\n" % (words_rdd, type(words_rdd), words_rdd.getNumPartitions()))

# repartition
words_rdd = words_rdd.repartition(16)

print("\nSpark RDD after repartition:\n\tobject=%s\n\ttype=%s\n\tnumPartitions=%d\n" % (words_rdd, type(words_rdd), words_rdd.getNumPartitions()))

# coalesce
words_rdd = words_rdd.coalesce(4)

print("\nSpark RDD after coalesce:\n\tobject=%s\n\ttype=%s\n\tnumPartitions=%d\n" % (words_rdd, type(words_rdd), words_rdd.getNumPartitions()))

# distinct
data = words_rdd.distinct().collect()

print("\nPython data after transformation distinct:\n\t%s\n\ttype=%s\n" % (data, type(data)))

# filter
data = words_rdd.filter(lambda w: w.startswith('t')).collect()

print("\nPython data after transformation filter:\n\t%s\n\ttype=%s\n" % (data, type(data)))

# zipWithIndex
data = words_rdd.zipWithIndex().collect()

print("\nPython data after transformation zipWithIndex:\n\t%s\n\ttype=%s\n" % (data, type(data)))

# union
tongue_twister_rdd = words_rdd.union(words_rdd)
data = tongue_twister_rdd.collect()

print("\nSpark RDD after union:\n\tobject=%s\n\ttype=%s\n\tnumPartitions=%d\n" % (tongue_twister_rdd, type(tongue_twister_rdd), tongue_twister_rdd.getNumPartitions()))
print("\nPython data after transformation union:\n\t%s\n\ttype=%s\n" % (data, type(data)))

# map
data = words_rdd.map(lambda w: w.upper() + '!').collect()

print("\nPython data after transformation map:\n\t%s\n\ttype=%s\n" % (data, type(data)))

data = words_rdd.map(lambda w: (w, 1)).collect()

print("\nPython data after transformation map:\n\t%s\n\ttype=%s\n" % (data, type(data)))

# flatMap
data = words_rdd.flatMap(lambda w: list(w)).collect()

print("\nPython data after transformation flatMap:\n\t%s\n\ttype=%s\n" % (data, type(data)))

# reduceByKey
data = tongue_twister_rdd.map(lambda w: (w, 1)).reduceByKey(lambda v1, v2: v1 + v2).collect()

print("\nPython data after transformation reduceByKey:\n\t%s\n\ttype=%s\n" % (data, type(data)))

# aggregateByKey
data = tongue_twister_rdd.map(lambda w: (w, 1)).aggregateByKey(
    0,                                                          # zero value 
    seqFunc = lambda par_sum, v: par_sum + v,                   # merge within a partion
    combFunc = lambda par_sum1, par_sum2: par_sum1 + par_sum2   # merge across partitions
).collect()

print("\nPython data after transformation aggregateByKey:\n\t%s\n\ttype=%s" % (data, type(data)))

# groupByKey
data = words_rdd.map(lambda w: (w, 1)).groupByKey().collect()

print("\nPython data after transformation groupByKey:\n\t%s\n\ttype=%s" % (data, type(data)))
print("\tactual data:", [(x[0], list(x[1])) for x in data])

# cogroup
l1 = [('a', 1), ('b', 2), ('c', 3)]
l2 = [('b', 4), ('b', 5), ('d', 6), ('c', 3)]
rdd1 = spark.sparkContext.parallelize(l1)
rdd2 = spark.sparkContext.parallelize(l2)
data = rdd1.cogroup(rdd2).collect()

print("\nPython data after transformation cogroup:\n\t%s\n\ttype=%s" % (data, type(data)))
print("\tactual data:", [(x[0], [list(xx) for xx in x[1]]) for x in data])

# join
data = rdd1.join(rdd2).collect()

print("\nPython data after transformation join:\n\t%s\n\ttype=%s" % (data, type(data)))

# cartesian
data = rdd1.cartesian(rdd2).collect()

print("\nPython data after transformation cartesian:\n\t%s\n\ttype=%s" % (data, type(data)))

# intersection
data = rdd1.intersection(rdd2).collect()

print("\nPython data after transformation intersection:\n\t%s\n\ttype=%s" % (data, type(data)))

# sortByKey
data = rdd1.sortByKey(ascending=False).collect()

print("\nPython data after transformation sortByKey:\n\t%s\n\ttype=%s" % (data, type(data)))
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('DISD').master('local[*]').getOrCreate()

numbers = list(range(10))
n_partitions = 4

numbers_rdd = spark.sparkContext.parallelize(numbers, n_partitions)

print("\nPython list:\n\t%s\n\ttype=%s\n" % (numbers, type(numbers)))
print("Spark RDD:\n\tobject=%s\n\ttype=%s\n\tnumPartitions=%d\n" % (numbers_rdd, type(numbers_rdd), numbers_rdd.getNumPartitions()))



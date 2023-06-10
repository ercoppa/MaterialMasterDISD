from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('DISD').master('local[*]').getOrCreate()

lines_rdd = spark.sparkContext.textFile('data/prova.txt')
lines = lines_rdd.collect()

print("Spark RDD:\n\tobject=%s\n\ttype=%s\n\tnumPartitions=%d\n" % (lines_rdd, type(lines_rdd), lines_rdd.getNumPartitions()))
print("\nPython data:\n\t%s\n\ttype=%s\n" % (lines, type(lines)))



from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('DISD').master('local[*]').getOrCreate()

files_rdd = spark.sparkContext.wholeTextFiles('data/')
files = files_rdd.collect()

print("Spark RDD:\n\tobject=%s\n\ttype=%s\n\tnumPartitions=%d\n" % (files_rdd, type(files_rdd), files_rdd.getNumPartitions()))
print("\nPython data:\n\t%s\n\ttype=%s\n" % (files, type(files)))



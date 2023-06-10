from pyspark.sql import SparkSession
import os
import sys

output_dir = "output"

spark = SparkSession.builder.appName('DISD').master('local[*]').getOrCreate()

lines_rdd = spark.sparkContext.textFile('data/prova.txt')
lines = lines_rdd.collect()

print("Spark RDD:\n\tobject=%s\n\ttype=%s\n\tnumPartitions=%d\n" % (lines_rdd, type(lines_rdd), lines_rdd.getNumPartitions()))
print("\nPython data:\n\t%s\n\ttype=%s\n" % (lines, type(lines)))

if os.path.exists(output_dir):
    print("Output directory already exists! Delete it.")
    sys.exit(1)

lines_rdd.saveAsTextFile(output_dir)

print([file for file in os.listdir(output_dir) if file[0] != '.'])
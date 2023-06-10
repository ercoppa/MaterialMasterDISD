from pyspark.sql import SparkSession

output_dir = 'output'

spark = SparkSession.builder.appName('DISD').master('local[*]').getOrCreate()

print()

lines = spark.sparkContext.textFile('input2/')
# lines = lines.sample(False, 0.1)
lines = lines.map(lambda x: x.split(" ")).map(lambda x: (x[0], 1))
lines = lines.reduceByKey(lambda x, y: x + y)
lines = lines.map(lambda x: (x[1], 1))
lines = lines.reduceByKey(lambda x, y: x + 1)
lines = lines.map(lambda x: str(x[0]) + "\t" + str(x[1]))

lines.saveAsTextFile(output_dir)
    
if False:
    print()
    print(lines.collect())
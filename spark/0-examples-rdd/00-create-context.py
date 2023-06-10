from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('DISD').master('local[*]').getOrCreate()

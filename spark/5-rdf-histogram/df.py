from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window

spark = SparkSession.builder.appName('DISD').master('local[*]').getOrCreate()

print()

if False:
    subject = StructField("subject", StringType(), nullable=False)
    predicate = StructField("predicate", StringType(), nullable=False)
    object = StructField("object", StringType(), nullable=False)
    context = StructField("context", StringType(), nullable=True)
    schema = StructType([subject, predicate, object, context])

    lines = spark.read.schema(schema).option(
        "delimiter", " ").csv('input/')
    lines = lines.groupBy('subject').count().withColumnRenamed('count', 'countSubjects')
    lines = lines.groupBy('countSubjects').count().withColumnRenamed('count', 'countFrequency')
    lines.write.mode("overwrite").format('csv').option(
        "delimiter", " ").option("header", "false").save('output')

else:

    subject = StructField("subject", StringType(), nullable=False)
    predicate = StructField("predicate", StringType(), nullable=False)
    object = StructField("object", StringType(), nullable=False)
    schema = StructType([subject, predicate, object])

    def split_data(data):
        data = data.split(" ")[:3]
        return data

    split_data_udf = udf(split_data, schema)
    
    lines = spark.read.text('input2/')

    lines = lines.select(split_data_udf('value').alias('triplet'))
    lines = lines.select('triplet.subject')
    lines = lines.groupBy('subject').count().withColumnRenamed('count', 'countSubjects')
    lines = lines.groupBy('countSubjects').count().withColumnRenamed('count', 'countFrequency')
    lines.write.mode("overwrite").format('csv').option(
        "delimiter", " ").option("header", "false").save('output')
    
if False:
    print(lines)
    lines.show()

# print(lines.collect())
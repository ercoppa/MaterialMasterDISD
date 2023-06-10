from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, column, expr
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('DISD').master('local[*]').getOrCreate()

print()

if False:
    subject = StructField("subject", StringType(), nullable=False)
    predicate = StructField("predicate", StringType(), nullable=False)
    object = StructField("object", StringType(), nullable=False)
    context = StructField("context", StringType(), nullable=True)
    schema = StructType([subject, predicate, object, context])

    lines = spark.read.schema(schema).option(
        "delimiter", " ").csv('input2/')

    lines.createOrReplaceTempView('lines_view')

    lines = spark.sql("SELECT count, COUNT(count) as frequency FROM (SELECT subject, COUNT(subject) AS count FROM lines_view GROUP BY subject) GROUP BY count")

    lines.write.mode("overwrite").format('csv').option(
        "delimiter", " ").option("header", "false").save('output')

else:
    subject = StructField("subject", StringType(), nullable=False)
    predicate = StructField("predicate", StringType(), nullable=False)
    object = StructField("object", StringType(), nullable=False)
    schema = StructType([subject, predicate, object])

    lines = spark.read.text('input2/')
    lines.createOrReplaceTempView('lines_view')

    def split_data(data):
        data = data.split(" ")[:3]
        return data

    split_data_udf = udf(split_data, schema)
    spark.udf.register("splitData", split_data_udf)

    lines = spark.sql("SELECT count, COUNT(count) as frequency FROM (SELECT subject, COUNT(subject) AS count FROM (SELECT triplet.subject AS subject FROM (SELECT splitData(value) AS triplet FROM lines_view)) GROUP BY subject) GROUP BY count")

    lines.write.mode("overwrite").format('csv').option(
        "delimiter", " ").option("header", "false").save('output')
    
if True:
    print(lines)
    lines.printSchema()
    lines.show()

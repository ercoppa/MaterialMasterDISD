from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, column, expr
from pyspark.sql.types import *

spark = SparkSession.builder.appName('DISD').master('local[*]').getOrCreate()

field_nome = StructField("nome", StringType(), nullable=False)
field_eta = StructField("eta", LongType(), nullable=False)
schema = StructType([field_nome, field_eta])

users = spark.read.schema(schema).option(
    "delimiter", "\t").csv('input/users.txt')
users = users.select("*").where('eta >= 18 and eta <= 25')

field_user = StructField("user", StringType(), nullable=False)
field_url = StructField("url", StringType(), nullable=False)
schema = StructType([field_user, field_url])

pages = spark.read.schema(schema).option(
    "delimiter", "\t").csv('input/pages.txt')
    
page_user = users.join(pages, on=users.nome == pages.user).selectExpr("url")
page_user = page_user.groupBy('url').count()

top = page_user.orderBy("count", ascending=False).take(5)

if True:
    print()
    users.printSchema()
    users.show()
    print()
    pages.printSchema()
    pages.show()
    print()
    page_user.printSchema()
    page_user.show()
    print()

print(top)

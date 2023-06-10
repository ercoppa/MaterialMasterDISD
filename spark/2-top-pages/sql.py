from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, column, expr
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('DISD').master('local[*]').getOrCreate()

print()

field_nome = StructField("nome", StringType(), nullable=False)
field_eta = StructField("eta", LongType(), nullable=False)
schema = StructType([field_nome, field_eta])

users = spark.read.schema(schema).option(
    "delimiter", "\t").csv('input/users.txt')

field_user = StructField("user", StringType(), nullable=False)
field_url = StructField("url", StringType(), nullable=False)
schema = StructType([field_user, field_url])

pages = spark.read.schema(schema).option(
    "delimiter", "\t").csv('input/pages.txt')

users.createOrReplaceTempView('users_view')
pages.createOrReplaceTempView('pages_view')

top = spark.sql('SELECT * FROM (SELECT url, COUNT(url) AS count FROM (SELECT U.nome, P.url FROM (SELECT * FROM users_view WHERE eta >= 18 AND eta <= 25) AS U INNER JOIN pages_view AS P ON U.nome = P.user) GROUP BY url) ORDER BY count DESC LIMIT 5')
    
print(top.collect())
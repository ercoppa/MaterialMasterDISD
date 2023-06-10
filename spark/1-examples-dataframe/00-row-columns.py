from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, column, expr

spark = SparkSession.builder.appName('DISD').master('local[*]').getOrCreate()

print()

# row

r = Row(0, 'Edoardo', 36, 'Roma')
print(r)

r = Row(id=0, nome='Edoardo', eta=36, luogo='Roma')
print(r)

# column

c = column('nome')
c = col('nome')         # same as column()
print(c)

# common checks performed in SQL over a column,
# which return an expression (which may involve 
# one or more columns)

print(c.alias('Nome'))
print(c.asc())
print(c.isNull())
print(c.startswith('Ma'))
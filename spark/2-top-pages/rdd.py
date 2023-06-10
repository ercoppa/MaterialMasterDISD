from pyspark.sql import SparkSession

output_dir = "output"

spark = SparkSession.builder.appName('DISD').master('local[*]').getOrCreate()

users = spark.sparkContext.textFile('input/users.txt')
users = users.map(lambda x : x.split("\t"))
users = users.map(lambda x : (x[0], int(x[1])))
users = users.filter(lambda x : x[1] >= 18 and x[1] <= 25)

pages = spark.sparkContext.textFile('input/pages.txt')
pages = pages.map(lambda x : x.split("\t"))

page_user = users.join(pages)
page_user = page_user.map(lambda x : (x[1][1], 1))

visits_by_page = page_user.reduceByKey(lambda x, y: x + y)

top = visits_by_page.top(5, key = lambda x: x[1])

if True: # debug only
    print()
    print(users.collect())
    print()
    print(pages.collect())
    print()
    print(page_user.collect())
    print()
    print(visits_by_page.collect())
    print()
    print(visits_by_page.getNumPartitions())
    print()

print(top)

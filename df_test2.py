from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("DFTest2") \
    .getOrCreate()

data = [
    (1, "Ashutosh"),
    (2, "Aniket")
]

df = spark.createDataFrame(
    data,
    ["id", "name"]
)

df.printSchema()

df.show()

spark.stop()
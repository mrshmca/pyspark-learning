from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("DFTest") \
    .getOrCreate()

print("Spark:", spark.version)

df = spark.range(10)

df.show()

spark.stop()
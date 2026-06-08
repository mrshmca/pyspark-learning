from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("WorkerTest") \
    .getOrCreate()

print("Spark Version:", spark.version)

rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])

print(rdd.collect())

spark.stop()
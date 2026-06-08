from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

print("Driver Python:", spark.sparkContext.pythonExec)
print("Spark Version:", spark.version)

spark.stop()
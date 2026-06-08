from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()

print("Spark Version:", spark.version)

data = [
    (1, "Ashutosh", "Patna"),
    (2, "Aniket", "Delhi"),
    (3, "Rahul", "Mumbai")
]

df = spark.createDataFrame(
    data,
    ["id", "name", "city"]
)

df.show()

spark.stop()
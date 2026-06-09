from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Read Gold")
    .getOrCreate()
)

df = spark.read.parquet(
    "data/gold/customer_transaction_totals"
)

df.show()

spark.stop()
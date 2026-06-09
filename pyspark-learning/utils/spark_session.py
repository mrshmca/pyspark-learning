# create reusable SparkSession for local PySpark development
from pyspark.sql import SparkSession

def create_spark_session():
    return SparkSession.builder \
        .appName("Local PySpark Development") \
        .master("local[*]") \
        .getOrCreate()
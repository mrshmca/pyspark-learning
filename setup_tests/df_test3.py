import os

python_path = r"C:\projects\copilot\.venv\Scripts\python.exe"

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("DFTest3")
    .config("spark.python.worker.faulthandler.enabled", "true")
    .getOrCreate()
)

print("Python Exec:", spark.sparkContext.pythonExec)

data = [
    (1, "Ashutosh"),
    (2, "Aniket")
]

df = spark.createDataFrame(data, ["id", "name"])

df.show()

spark.stop()
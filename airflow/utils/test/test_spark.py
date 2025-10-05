# spark_test.py
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master('spark://spark-iceberg:7077')
    .appName("Running_Spark_Airflow_Test")
    .getOrCreate()
)

data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
columns = ["name", "value"]

df = spark.createDataFrame(data, columns)
df.show()

spark.stop()
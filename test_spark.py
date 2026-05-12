from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test-spark").getOrCreate()

print("Spark is running")
print("Spark version:", spark.version)

spark.stop()
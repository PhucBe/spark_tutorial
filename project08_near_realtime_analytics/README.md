$env:HADOOP_HOME = "C:\hadoop"
$env:PATH = "C:\hadoop\bin;$env:PATH"
$env:PYSPARK_PYTHON = "$PWD\.venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "$PWD\.venv\Scripts\python.exe"
$env:PYTHONPATH = "$PWD"
$env:SPARK_LOCAL_IP = "127.0.0.1"

.\.venv\Scripts\spark-submit.cmd `
  --conf spark.driver.host=127.0.0.1 `
  --conf spark.driver.bindAddress=127.0.0.1 `
  --conf spark.ui.host=127.0.0.1 `
  --conf spark.master=local[2] `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 `
  src/streaming/stream_curated_metrics.py
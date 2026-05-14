$PROJECT_ROOT = "D:\DataEngineer\spark_tutorial\week07_spark_streaming"

Set-Location $PROJECT_ROOT

$env:SPARK_HOME = "D:\DataEngineer\spark_tutorial\.venv\Lib\site-packages\pyspark"
$env:PYSPARK_PYTHON = "D:\DataEngineer\spark_tutorial\.venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "D:\DataEngineer\spark_tutorial\.venv\Scripts\python.exe"
$env:PYTHONPATH = $PROJECT_ROOT

& "$env:SPARK_HOME\bin\spark-submit.cmd" `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 `
  src\streaming\stream_order_metrics.py
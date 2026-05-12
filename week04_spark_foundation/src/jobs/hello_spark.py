from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("week04-hello-spark").getOrCreate()

    data = [
        ("O1001", "C001", 120.5),
        ("O1002", "C002", 88.0),
        ("O1003", "C003", 245.0),
    ]

    df = spark.createDataFrame(data, ["order_id", "customer_id", "amount"])

    df.printSchema()
    df.show()

    spark.stop()


if __name__ == "__main__":
    main()
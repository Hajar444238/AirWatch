from pyspark.sql import SparkSession, functions as F

TOPIC = "air-quality-test"

spark = (
    SparkSession.builder.appName("AirWatchStreaming")
    .config("spark.sql.session.timeZone", "Asia/Shanghai")
    .getOrCreate()
)

schema = """
    `No` BIGINT, year INT, month INT, day INT, hour INT,
    `PM2.5` DOUBLE, PM10 DOUBLE, SO2 DOUBLE, NO2 DOUBLE,
    CO DOUBLE, O3 DOUBLE, TEMP DOUBLE, PRES DOUBLE,
    DEWP DOUBLE, RAIN DOUBLE, wd STRING, WSPM DOUBLE,
    station STRING, source STRING
"""

messages = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

events = (
    messages.select(
        F.from_json(F.col("value").cast("string"), schema).alias("mesure")
    )
    .select("mesure.*")
    .withColumnRenamed("PM2.5", "pm25")
    .withColumn("event_time", F.expr("make_timestamp(year, month, day, hour, 0, 0)"))
)

query = (
    events.writeStream
    .format("console")
    .outputMode("append")
    .option("checkpointLocation", "hdfs://namenode:9000/airwatch/checkpoints/streaming_test")
    .start()
)

query.awaitTermination()
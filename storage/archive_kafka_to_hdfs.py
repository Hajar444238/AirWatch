from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder.appName("AirWatchArchive")
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
    .option("subscribe", "air-quality")
    .option("startingOffsets", "earliest")
    .load()
)

observations = (
    messages.select(
        F.from_json(F.col("value").cast("string"), schema).alias("mesure"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    )
    .select("mesure.*", "kafka_partition", "kafka_offset")
    .withColumnRenamed("PM2.5", "pm25")
    .withColumn("event_time", F.expr("make_timestamp(year, month, day, hour, 0, 0)"))
    .filter(F.col("event_time").isNotNull() & (F.length(F.trim("station")) > 0))
    .withColumn("event_date", F.to_date("event_time"))
)

query = (
    observations.writeStream.format("parquet")
    .outputMode("append")
    .partitionBy("event_date")
    .option("path", "hdfs://namenode:9000/airwatch/observations")
    .option("checkpointLocation", "hdfs://namenode:9000/airwatch/checkpoints/archive")
    .start()
)

query.awaitTermination()

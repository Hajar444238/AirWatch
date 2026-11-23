from pyspark.sql import SparkSession, functions as F
from pyspark.ml import PipelineModel

TOPIC = "air-quality-test"
MODEL_PATH = "hdfs://namenode:9000/airwatch/models/provisional_lr"

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
    station STRING, source STRING,
    pm25_lag1 DOUBLE, pm25_lag2 DOUBLE, pm25_lag3 DOUBLE
"""

# Le modèle est chargé une seule fois, au démarrage
model = PipelineModel.load(MODEL_PATH)

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

# Prédiction de PM2.5 pour l'heure suivante
predictions = model.transform(events).select(
    "station", "event_time", "pm25", F.col("prediction").alias("pm25_predit_t_plus_1h")
)

query = (
    predictions.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("checkpointLocation", "hdfs://namenode:9000/airwatch/checkpoints/streaming_predict_v1")
    .start()
)

query.awaitTermination()
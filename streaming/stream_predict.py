import math

import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import PipelineModel

TOPIC = "air-quality-test"
MODEL_PATH = "hdfs://namenode:9000/airwatch/models/provisional_lr"
CHECKPOINT = "hdfs://namenode:9000/airwatch/checkpoints/streaming_predict_v5"

JDBC_URL = "jdbc:postgresql://postgres:5432/airwatch"
JDBC_PROPS = {"user": "airwatch", "password": "airwatch", "driver": "org.postgresql.Driver"}

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

# Le modèle est chargé une seule fois, au démarrage
model = PipelineModel.load(MODEL_PATH)

messages = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# 1. Décodage ; on garde l'heure d'arrivée du message dans Kafka
events = (
    messages.select(
        F.from_json(F.col("value").cast("string"), schema).alias("mesure"),
        F.col("timestamp").alias("kafka_time"),
    )
    .select("mesure.*", "kafka_time")
    .withColumnRenamed("PM2.5", "pm25")
    .withColumn("event_time", F.expr("make_timestamp(year, month, day, hour, 0, 0)"))
    .filter(F.col("event_time").isNotNull() & F.col("station").isNotNull())
    .select("station", "event_time", "pm25", "PM10", "NO2", "CO", "O3",
            "TEMP", "PRES", "WSPM", "hour", "month", "kafka_time")
)

OUT_SCHEMA = (
    "station STRING, event_time TIMESTAMP, pm25 DOUBLE, PM10 DOUBLE, "
    "NO2 DOUBLE, CO DOUBLE, O3 DOUBLE, TEMP DOUBLE, PRES DOUBLE, "
    "WSPM DOUBLE, hour INT, month INT, kafka_time TIMESTAMP, "
    "pm25_lag1 DOUBLE, pm25_lag2 DOUBLE, pm25_lag3 DOUBLE"
)
STATE_SCHEMA = "h1 DOUBLE, h2 DOUBLE, h3 DOUBLE"


# 2. Pour chaque station : on retrouve les 3 dernières valeurs, on calcule les lags
def add_lags(key, pdf_iter, state):
    nan = math.nan
    if state.exists:
        h1, h2, h3 = [nan if x is None else x for x in state.get]
    else:
        h1, h2, h3 = nan, nan, nan

    pdf = pd.concat(list(pdf_iter)).sort_values("event_time")
    lag1, lag2, lag3 = [], [], []
    for valeur in pdf["pm25"]:
        lag1.append(h1)
        lag2.append(h2)
        lag3.append(h3)
        h3, h2, h1 = h2, h1, valeur

    pdf["pm25_lag1"] = lag1
    pdf["pm25_lag2"] = lag2
    pdf["pm25_lag3"] = lag3
    state.update((h1, h2, h3))
    yield pdf


with_lags = events.groupBy("station").applyInPandasWithState(
    add_lags, OUT_SCHEMA, STATE_SCHEMA, "append", "NoTimeout"
)

# 3. Prédiction de PM2.5 pour l'heure suivante, et mesure du délai
predictions = (
    model.transform(with_lags)
    .select(
        "station",
        "event_time",
        "pm25",
        F.col("prediction").alias("pm25_predit_t_plus_1h"),
        (F.col("event_time") + F.expr("INTERVAL 1 HOUR")).alias("heure_cible"),
        "kafka_time",
        F.current_timestamp().alias("processed_at"),
    )
    .withColumn("delai_s", F.col("processed_at").cast("double") - F.col("kafka_time").cast("double"))
)


# 4. Écriture de chaque lot dans PostgreSQL.
#    Si un lot est rejoué après une panne, la base ignore les lignes déjà présentes.
def write_batch(batch_df, batch_id):
    (
        batch_df.withColumn("batch_id", F.lit(batch_id))
        .write.jdbc(JDBC_URL, "predictions", mode="append", properties=JDBC_PROPS)
    )


query = (
    predictions.writeStream
    .foreachBatch(write_batch)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .start()
)

query.awaitTermination()
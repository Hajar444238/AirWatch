from pyspark.sql import SparkSession, Window, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

MODEL_PATH = "hdfs://namenode:9000/airwatch/models/provisional_lr"

spark = (
    SparkSession.builder.appName("AirWatchTrainProvisional")
    .config("spark.sql.session.timeZone", "Asia/Shanghai")
    .getOrCreate()
)

# 1. Lecture de l'historique archivé dans HDFS
df = spark.read.parquet("hdfs://namenode:9000/airwatch/observations")

# 2. Fenêtre : les mesures d'une même station, triées par heure
w = Window.partitionBy("station").orderBy("event_time")

# 3. Variables décalées (lags) et cible (PM2.5 de l'heure suivante)
data = (
    df.withColumn("pm25_lag1", F.lag("pm25", 1).over(w))
    .withColumn("pm25_lag2", F.lag("pm25", 2).over(w))
    .withColumn("pm25_lag3", F.lag("pm25", 3).over(w))
    .withColumn("pm25_next", F.lead("pm25", 1).over(w))
)

FEATURES = [
    "pm25", "pm25_lag1", "pm25_lag2", "pm25_lag3",
    "PM10", "NO2", "CO", "O3", "TEMP", "PRES", "WSPM",
    "hour", "month",
]

# 4. On écarte les lignes où une variable ou la cible manque
data = data.dropna(subset=FEATURES + ["pm25_next"])

# 5. Découpage dans le temps : entraînement avant mars 2016, test après
limite = F.to_timestamp(F.lit("2016-03-01 00:00:00"))
train = data.filter(F.col("event_time") < limite)
test = data.filter(F.col("event_time") >= limite)
print("Lignes entrainement :", train.count(), "| Lignes test :", test.count())

# 6. Pipeline = assemblage des variables + régression linéaire
assembler = VectorAssembler(inputCols=FEATURES, outputCol="features", handleInvalid="skip")
lr = LinearRegression(featuresCol="features", labelCol="pm25_next")
model = Pipeline(stages=[assembler, lr]).fit(train)

# 7. Évaluation sur le test
pred = model.transform(test)
mae = RegressionEvaluator(labelCol="pm25_next", predictionCol="prediction", metricName="mae").evaluate(pred)
rmse = RegressionEvaluator(labelCol="pm25_next", predictionCol="prediction", metricName="rmse").evaluate(pred)

# 8. Référence simple : la persistance (on prédit PM2.5(t+1) = PM2.5(t))
ref = test.select(
    F.avg(F.abs(F.col("pm25") - F.col("pm25_next"))).alias("mae"),
    F.sqrt(F.avg(F.pow(F.col("pm25") - F.col("pm25_next"), 2))).alias("rmse"),
).first()

print("=== RESULTATS ===")
print("Modele   : MAE =", round(mae, 2), "| RMSE =", round(rmse, 2))
print("Reference: MAE =", round(ref["mae"], 2), "| RMSE =", round(ref["rmse"], 2))

# 9. Sauvegarde du pipeline entier dans HDFS
model.write().overwrite().save(MODEL_PATH)
print("Modele sauvegarde dans", MODEL_PATH)
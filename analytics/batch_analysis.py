from pyspark.sql import SparkSession, functions as F
from pyspark.sql import Window

# =====================================================
# 1. CONFIGURATION
# =====================================================

RAW_PATH = "hdfs://namenode:9000/airwatch/observations"

SILVER_PATH = "hdfs://namenode:9000/airwatch/analytics/silver/observations"

GOLD_PATH = "hdfs://namenode:9000/airwatch/analytics/gold"

spark = (
    SparkSession.builder
    .appName("AirWatch-Data-Analysis")
    .config("spark.sql.session.timeZone", "Asia/Shanghai")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


def save_csv(dataframe, name):
    """Save an analytical result as a CSV directory in HDFS."""(.venv) PS C:\Users\ADMIN\Desktop\AirWatch> docker compose -f infra/docker-compose.yml exec namenode hdfs dfs -ls -R /airwatch/analytics/gold
ls: `/airwatch/analytics/gold': No such file or directory

What's next:
    Debug this Compose error with Gordon → docker ai "help me fix this compose error"
    path = f"{GOLD_PATH}/{name}"

    (
        dataframe.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(path)
    )

    print(f"Saved: {path}")


# =====================================================
# 2. DATA LOADING
# =====================================================

print("\n===== DATA LOADING =====")

raw = spark.read.parquet(RAW_PATH)

print("Schema:")
raw.printSchema()

total_rows = raw.count()

print("Total observations:", total_rows)

print("Number of stations:",
      raw.select("station").distinct().count())


# =====================================================
# 3. DATA QUALITY ANALYSIS
# =====================================================

print("\n===== DATA QUALITY =====")

# Missing values

missing_rows = []

for column in raw.columns:

    count_missing = raw.filter(
        F.col(column).isNull()
    ).count()

    percentage = (
        count_missing / total_rows * 100
        if total_rows else 0
    )

    missing_rows.append((
        column,
        count_missing,
        percentage
    ))

missing_df = spark.createDataFrame(
    missing_rows,
    ["variable", "missing_count", "missing_percentage"]
)

missing_df.show(truncate=False)

save_csv(missing_df, "missing_values")


# Duplicate observations

duplicate_groups = (
    raw.groupBy("station", "event_time")
    .count()
    .filter(F.col("count") > 1)
)

duplicate_count = (
    duplicate_groups
    .agg(F.sum(F.col("count") - 1).alias("duplicates"))
    .first()["duplicates"]
)

print("Duplicate observations:", duplicate_count or 0)


# Invalid observations

invalid_count = raw.filter(
    F.col("event_time").isNull() |
    F.col("station").isNull() |
    (F.length(F.trim(F.col("station"))) == 0)
).count()

print("Invalid station/time observations:", invalid_count)


# =====================================================
# 4. DATA PREPROCESSING
# =====================================================

print("\n===== DATA PREPROCESSING =====")

clean = (
    raw
    .filter(F.col("event_time").isNotNull())
    .filter(F.col("station").isNotNull())
    .filter(F.length(F.trim(F.col("station"))) > 0)
    .dropDuplicates(["station", "event_time"])
)

clean_count = clean.count()

print("Clean observations:", clean_count)

# Keep original data intact.
# Do not automatically remove outliers or impute missing
# pollutant values without a documented justification.

clean.write.mode("overwrite").parquet(SILVER_PATH)

print("Silver data saved:", SILVER_PATH)


# =====================================================
# 5. DESCRIPTIVE STATISTICS
# =====================================================

print("\n===== DESCRIPTIVE STATISTICS =====")

numeric_columns = [
    "pm25", "PM10", "SO2", "NO2",
    "CO", "O3", "TEMP", "PRES",
    "DEWP", "RAIN", "WSPM"
]

available_columns = [
    c for c in numeric_columns if c in clean.columns
]

statistics = clean.select(
    *available_columns
).describe()

statistics.show()

save_csv(statistics, "descriptive_statistics")


# =====================================================
# 6. OUTLIER THRESHOLDS
# =====================================================

print("\n===== OUTLIER ANALYSIS =====")

outlier_rows = []

for column in available_columns:

    quantiles = clean.approxQuantile(
        column,
        [0.25, 0.75],
        0.01
    )

    if len(quantiles) == 2:

        q1, q3 = quantiles
        iqr = q3 - q1

        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr

        outlier_count = clean.filter(
            (F.col(column) < lower) |
            (F.col(column) > upper)
        ).count()

        outlier_rows.append((
            column,
            q1,
            q3,
            lower,
            upper,
            outlier_count
        ))

outlier_df = spark.createDataFrame(
    outlier_rows,
    [
        "variable", "q1", "q3",
        "lower_bound", "upper_bound",
        "outlier_count"
    ]
)

outlier_df.show()

save_csv(outlier_df, "outlier_analysis")


# =====================================================
# 7. TEMPORAL ANALYSIS
# =====================================================

print("\n===== TEMPORAL ANALYSIS =====")

monthly = (
    clean
    .groupBy("year", "month")
    .agg(
        F.avg("pm25").alias("avg_pm25"),
        F.avg("PM10").alias("avg_pm10"),
        F.count("*").alias("observations")
    )
    .orderBy("year", "month")
)

save_csv(monthly, "monthly_pm25")


hourly = (
    clean
    .groupBy("hour")
    .agg(
        F.avg("pm25").alias("avg_pm25"),
        F.count("*").alias("observations")
    )
    .orderBy("hour")
)

save_csv(hourly, "hourly_pm25")


# Seasonal analysis

seasonal = (
    clean
    .withColumn(
        "season",
        F.when(F.col("month").isin(12, 1, 2), "Winter")
        .when(F.col("month").isin(3, 4, 5), "Spring")
        .when(F.col("month").isin(6, 7, 8), "Summer")
        .otherwise("Autumn")
    )
    .groupBy("season")
    .agg(F.avg("pm25").alias("avg_pm25"))
)

save_csv(seasonal, "seasonal_pm25")


# =====================================================
# 8. STATION ANALYSIS
# =====================================================

print("\n===== STATION ANALYSIS =====")

station_stats = (
    clean
    .groupBy("station")
    .agg(
        F.count("*").alias("observations"),
        F.avg("pm25").alias("avg_pm25"),
        F.avg("PM10").alias("avg_pm10"),
        F.avg("NO2").alias("avg_no2"),
        F.avg("SO2").alias("avg_so2"),
        F.avg("CO").alias("avg_co"),
        F.avg("O3").alias("avg_o3")
    )
    .orderBy(F.desc("avg_pm25"))
)

station_stats.show()

save_csv(station_stats, "station_statistics")


# =====================================================
# 9. CORRELATION ANALYSIS
# =====================================================

print("\n===== CORRELATION ANALYSIS =====")

correlation_variables = [
    "pm25", "PM10", "SO2", "NO2",
    "CO", "O3", "TEMP", "PRES",
    "DEWP", "RAIN", "WSPM"
]

correlation_variables = [
    c for c in correlation_variables
    if c in clean.columns
]

correlation_rows = []

for i, variable1 in enumerate(correlation_variables):

    for variable2 in correlation_variables[i:]:

        value = clean.stat.corr(
            variable1,
            variable2,
            method="pearson"
        )

        correlation_rows.append((
            variable1,
            variable2,
            value
        ))

correlation_df = spark.createDataFrame(
    correlation_rows,
    ["variable_1", "variable_2", "pearson_correlation"]
)

correlation_df.show(truncate=False)

save_csv(correlation_df, "correlations")


# =====================================================
# 10. PM2.5 SAMPLE FOR VISUALIZATION
# =====================================================

sample = (
    clean
    .select("pm25", "station")
    .filter(F.col("pm25").isNotNull())
    .sample(withReplacement=False, fraction=0.10, seed=42)
    .limit(50000)
)

save_csv(sample, "pm25_sample")


# Check temporal continuity by station

window = (
    Window
    .partitionBy("station")
    .orderBy("event_time")
)

gaps = (
    clean
    .withColumn(
        "previous_event_time",
        F.lag("event_time", 1).over(window)
    )
    .withColumn(
        "gap_hours",
        (
            F.unix_timestamp("event_time") -
            F.unix_timestamp("previous_event_time")
        ) / 3600
    )
    .filter(
        F.col("previous_event_time").isNotNull()
    )
    .filter(F.col("gap_hours") != 1)
)

print("Non-hourly intervals:", gaps.count())

save_csv(gaps, "temporal_gaps")


# =====================================================
# 11. FEATURE ENGINEERING
# =====================================================

print("\n===== FEATURE ENGINEERING =====")

window = (
    Window
    .partitionBy("station")
    .orderBy("event_time")
)

features = (
    clean
    .withColumn("pm25_lag1", F.lag("pm25", 1).over(window))
    .withColumn("pm25_lag2", F.lag("pm25", 2).over(window))
    .withColumn("pm25_lag3", F.lag("pm25", 3).over(window))
    .withColumn("pm25_next", F.lead("pm25", 1).over(window))
)

features = features.dropna(
    subset=["pm25", "pm25_lag1", "pm25_next"]
)

features.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/airwatch/analytics/gold/features"
)

print("Feature dataset saved.")


# =====================================================
# 12. FINAL SUMMARY
# =====================================================

print("\n===== FINAL SUMMARY =====")

print("Raw observations:", total_rows)
print("Clean observations:", clean_count)
print("Stations:", clean.select("station").distinct().count())
print("Duplicates:", duplicate_count or 0)

print("\nAnalytics completed successfully.")

spark.stop()

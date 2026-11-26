
from pathlib import Path
import glob

import pandas as pd
import matplotlib.pyplot as plt


# =====================================================
# CONFIGURATION
# =====================================================

BASE_DIR = Path(__file__).resolve().parents[1]

DATA_DIR = BASE_DIR / "outputs" / "aggregated_data" / "airwatch_gold"

FIGURES_DIR = BASE_DIR / "outputs" / "figures"

FIGURES_DIR.mkdir(parents=True, exist_ok=True)


# =====================================================
# CSV LOADER
# =====================================================

def load_spark_csv(folder_name):

    folder = DATA_DIR / folder_name

    files = glob.glob(str(folder / "part-*.csv"))

    if not files:
        raise FileNotFoundError(
            f"No Spark CSV found in {folder}"
        )

    return pd.concat(
        [pd.read_csv(file) for file in files],
        ignore_index=True
    )


# =====================================================
# 1. PM2.5 DISTRIBUTION
# =====================================================

sample = load_spark_csv("pm25_sample")

sample["pm25"] = pd.to_numeric(
    sample["pm25"],
    errors="coerce"
)

plt.figure(figsize=(10, 5))

plt.hist(
    sample["pm25"].dropna(),
    bins=50
)

plt.title("Distribution des concentrations PM2.5")
plt.xlabel("Concentration PM2.5")
plt.ylabel("Fréquence")

plt.tight_layout()

plt.savefig(
    FIGURES_DIR / "pm25_distribution.png",
    dpi=300
)

plt.close()


# =====================================================
# 2. MONTHLY ANALYSIS
# =====================================================

monthly = load_spark_csv("monthly_pm25")

monthly["year"] = pd.to_numeric(monthly["year"])
monthly["month"] = pd.to_numeric(monthly["month"])

monthly["period"] = pd.to_datetime(
    dict(
        year=monthly["year"],
        month=monthly["month"],
        day=1
    )
)

monthly = monthly.sort_values("period")

plt.figure(figsize=(13, 5))

plt.plot(
    monthly["period"],
    monthly["avg_pm25"],
    marker="o"
)

plt.title("Évolution mensuelle de PM2.5")
plt.xlabel("Période")
plt.ylabel("PM2.5 moyen")

plt.xticks(rotation=45)
plt.grid(alpha=0.3)
plt.tight_layout()

plt.savefig(
    FIGURES_DIR / "monthly_pm25.png",
    dpi=300
)

plt.close()


# =====================================================
# 3. HOURLY ANALYSIS
# =====================================================

hourly = load_spark_csv("hourly_pm25")

hourly = hourly.sort_values("hour")

plt.figure(figsize=(10, 5))

plt.plot(
    hourly["hour"],
    hourly["avg_pm25"],
    marker="o"
)

plt.title("Variation horaire de PM2.5")
plt.xlabel("Heure")
plt.ylabel("PM2.5 moyen")

plt.xticks(range(24))
plt.grid(alpha=0.3)
plt.tight_layout()

plt.savefig(
    FIGURES_DIR / "hourly_pm25.png",
    dpi=300
)

plt.close()


# =====================================================
# 4. STATION COMPARISON
# =====================================================

stations = load_spark_csv("station_statistics")

stations = stations.sort_values(
    "avg_pm25",
    ascending=True
)

plt.figure(figsize=(11, 7))

plt.barh(
    stations["station"],
    stations["avg_pm25"]
)

plt.title("Comparaison des stations selon PM2.5")
plt.xlabel("PM2.5 moyen")
plt.ylabel("Station")

plt.tight_layout()

plt.savefig(
    FIGURES_DIR / "station_comparison.png",
    dpi=300
)

plt.close()


# =====================================================
# 5. CORRELATION MATRIX
# =====================================================

correlations = load_spark_csv("correlations")

matrix = correlations.pivot(
    index="variable_1",
    columns="variable_2",
    values="pearson_correlation"
)

matrix = matrix.combine_first(matrix.T)

plt.figure(figsize=(11, 9))

image = plt.imshow(
    matrix,
    aspect="auto",
    vmin=-1,
    vmax=1
)

plt.colorbar(image, label="Pearson correlation")

plt.xticks(
    range(len(matrix.columns)),
    matrix.columns,
    rotation=45,
    ha="right"
)

plt.yticks(
    range(len(matrix.index)),
    matrix.index
)

plt.title("Matrice de corrélation des variables")

plt.tight_layout()

plt.savefig(
    FIGURES_DIR / "correlation_matrix.png",
    dpi=300
)

plt.close()


# =====================================================
# 6. MISSING VALUES
# =====================================================

missing = load_spark_csv("missing_values")

missing = missing.sort_values(
    "missing_percentage",
    ascending=True
)

plt.figure(figsize=(10, 7))

plt.barh(
    missing["variable"],
    missing["missing_percentage"]
)

plt.title("Pourcentage de valeurs manquantes")
plt.xlabel("Pourcentage (%)")

plt.tight_layout()

plt.savefig(
    FIGURES_DIR / "missing_values.png",
    dpi=300
)

plt.close()


print("All visualizations generated successfully.")
print("Output directory:", FIGURES_DIR)

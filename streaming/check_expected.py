import sys
import pandas as pd

df = pd.read_csv(sys.argv[1]).head(int(sys.argv[2]))
cols = ["PM2.5", "PM10", "NO2", "CO", "O3", "TEMP", "PRES", "WSPM"]
for k in (1, 2, 3):
    df[f"lag{k}"] = df["PM2.5"].shift(k)

ok = df[cols + ["lag1", "lag2", "lag3"]].notna().all(axis=1)
print("Predictions attendues sans panne :", int(ok.sum()))
print("Heures sans prediction possible :")
print(df.loc[~ok, ["year", "month", "day", "hour"]].iloc[3:].to_string(index=False))
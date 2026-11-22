import json
import sys
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

TOPIC = "air-quality-test"
DELAY = 1.0  # secondes entre deux messages

csv_file = Path(sys.argv[1])  # chemin du CSV donné en argument

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v, allow_nan=False).encode("utf-8"),
)

df = pd.read_csv(csv_file)
station = csv_file.stem.replace("PRSA_Data_", "").replace("_20130301-20170228", "")
df["station"] = station

# Test court : on ne garde que les 60 premières mesures
df = df.head(60)

for row in df.to_dict("records"):
    event = {k: None if pd.isna(v) else v for k, v in row.items()}
    event["source"] = "AirWatch"

    producer.send(TOPIC, value=event)
    print(event["station"], event["year"], event["month"], event["day"], event["hour"])
    time.sleep(DELAY)

producer.flush()
producer.close()
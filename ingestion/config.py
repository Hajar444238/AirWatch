from pathlib import Path

# Chemin racine du projet AirWatch
BASE_DIR = Path(__file__).resolve().parent.parent

# Dossier contenant les fichiers CSV
DATA_DIR = BASE_DIR / "data"

# Configuration Kafka
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "air-quality"

# Configuration de l'ingestion
BATCH_SIZE = 100
DELAY_SECONDS = 0

# Fichier CSV utilisé pour le premier test
CSV_FILES = sorted(DATA_DIR.glob("PRSA_Data_*.csv"))
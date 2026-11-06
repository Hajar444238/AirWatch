import json
import time
import logging

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import (
    CSV_FILES,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    DELAY_SECONDS
)


# -----------------------------
# CONFIGURATION DU LOGGER
# -----------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("AirWatchProducer")


# -----------------------------
# CREATION DU PRODUCER KAFKA
# -----------------------------

def create_producer():

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

        value_serializer=lambda value: json.dumps(
            value,
            allow_nan=False
        ).encode("utf-8"),

        acks="all",
        retries=5
    )

    logger.info("Connexion a Kafka reussie")

    return producer


# -----------------------------
# LECTURE DU DATASET
# -----------------------------

def read_datasets():
    if not CSV_FILES:
        raise FileNotFoundError("Aucun fichier CSV trouve dans data/")

    datasets = []

    for csv_file in CSV_FILES:
        logger.info(f"Lecture du fichier : {csv_file.name}")

        df = pd.read_csv(csv_file)

        station = csv_file.stem.replace("PRSA_Data_", "").replace(
            "_20130301-20170228", ""
        )

        df["station"] = station
        datasets.append(df)

        logger.info(
            f"{len(df)} observations lues pour la station {station}"
        )

    all_data = pd.concat(datasets, ignore_index=True)

    logger.info(f"Total des observations : {len(all_data)}")

    return all_data


# -----------------------------
# ENVOI DES DONNEES A KAFKA
# -----------------------------

def publish_data(producer, df):

    total_sent = 0

    for index, row in df.iterrows():

        # Conversion de la ligne CSV en dictionnaire
        observation = row.to_dict()

        # Remplacement des valeurs manquantes
        observation = {
            key: None if pd.isna(value) else value
            for key, value in observation.items()
        }

        # Ajout d'un identifiant de station
        observation["source"] = "AirWatch"

        try:

            future = producer.send(
                KAFKA_TOPIC,
                value=observation
            )

            # Attendre la confirmation de Kafka
            metadata = future.get(timeout=10)

            total_sent += 1

            logger.info(
                f"Message {total_sent} envoye | "
                f"Partition : {metadata.partition} | "
                f"Offset : {metadata.offset}"
            )

            # Simulation d'une arrivée progressive
            if DELAY_SECONDS > 0:
                time.sleep(DELAY_SECONDS)

        except KafkaError as error:

            logger.error(
                f"Erreur lors de l'envoi : {error}"
            )

    producer.flush()

    logger.info(
        f"Ingestion terminee : {total_sent} messages envoyes"
    )


# -----------------------------
# PROGRAMME PRINCIPAL
# -----------------------------

def main():

    producer = None

    try:

        logger.info("Demarrage du Producer AirWatch")

        df = read_datasets()

        producer = create_producer()

        publish_data(producer, df)

    except Exception as error:

        logger.exception(
            f"Erreur dans le Producer : {error}"
        )

    finally:

        if producer is not None:
            producer.close()

        logger.info("Arret du Producer AirWatch")


if __name__ == "__main__":
    main()
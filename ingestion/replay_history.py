"""Replay the AirWatch historical CSVs to Kafka with bounded batches."""

import argparse
from itertools import islice
from pathlib import Path

import pandas as pd

from config import KAFKA_TOPIC
from producer import create_producer, logger, read_datasets

SAMPLE_FILE = "PRSA_Data_Aotizhongxin_20130301-20170228.csv"
BATCH_SIZE = 500


def replay_file(producer, csv_file, skip_test_sample):
    observations = read_datasets([csv_file])
    if skip_test_sample and csv_file.name == SAMPLE_FILE:
        observations = observations.loc[~observations["No"].between(1, 10)]

    rows = iter(observations.to_dict("records"))
    count = 0
    while batch := list(islice(rows, BATCH_SIZE)):
        pending = []
        for row in batch:
            event = {key: None if pd.isna(value) else value for key, value in row.items()}
            event["source"] = "AirWatch"
            pending.append(producer.send(KAFKA_TOPIC, value=event))
        for acknowledgement in pending:
            acknowledgement.get(timeout=30)
        count += len(pending)
    return count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_directory", type=Path)
    parser.add_argument(
        "--skip-test-sample",
        action="store_true",
        help="Skip the first ten Aotizhongxin measurements already published in the storage test.",
    )
    args = parser.parse_args()
    csv_files = sorted(args.csv_directory.glob("PRSA_Data_*.csv"))
    if not csv_files:
        parser.error("No PRSA_Data_*.csv files found in the given directory.")

    producer = create_producer()
    try:
        total_sent = 0
        for csv_file in csv_files:
            count = replay_file(producer, csv_file, args.skip_test_sample)
            total_sent += count
            logger.info("%s : %s messages confirmes", csv_file.name, count)
        logger.info("Historique publie : %s messages confirmes", total_sent)
    finally:
        producer.close()


if __name__ == "__main__":
    main()

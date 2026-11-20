# Archivage Kafka vers HDFS

Le job `archive_kafka_to_hdfs.py` consomme les observations JSON du topic `air-quality` avec Spark Structured Streaming et les écrit en Parquet dans `hdfs://namenode:9000/airwatch/observations`. Les données sont partitionnées par date de mesure à Pékin. Le checkpoint `hdfs://namenode:9000/airwatch/checkpoints/archive` permet de reprendre depuis les offsets Kafka déjà traités.

## Démarrage

Depuis la racine du dépôt, avec Docker Desktop en mode Linux et Python 3.12 disponibles, exécuter ces commandes PowerShell :

```powershell
docker compose -f infra/docker-compose.yml up -d
docker compose -f infra/docker-compose.yml exec namenode hdfs dfsadmin -report
docker compose -f infra/docker-compose.yml exec namenode hdfs dfs -mkdir -p /airwatch
docker compose -f infra/docker-compose.yml exec namenode hdfs dfs -chown spark /airwatch
docker compose -f infra/docker-compose.yml exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic air-quality --partitions 1 --replication-factor 1
docker compose -f infra/docker-compose.yml cp storage/archive_kafka_to_hdfs.py spark-master:/tmp/archive_kafka_to_hdfs.py
docker compose -f infra/docker-compose.yml exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.driver.host=spark-master --conf spark.cores.max=1 --conf spark.hadoop.dfs.replication=2 --conf spark.jars.ivy=/tmp/airwatch-ivy --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 /tmp/archive_kafka_to_hdfs.py
```

Vérifier que HDFS affiche `Live datanodes (2)` et que le [master Spark](http://localhost:18080) affiche `Alive Workers: 2`. Laisser le job d'archivage tourner dans son terminal. Il utilise un cœur, ce qui laisse l'autre disponible pour la vérification SQL.

## Charger l'historique

Extraire les 12 fichiers `PRSA_Data_*.csv` du [dataset UCI Beijing Multi-Site Air Quality](https://archive.ics.uci.edu/dataset/501/beijing+multi+site+air+quality+data) sous licence CC BY 4.0. Dans un autre terminal, indiquer le dossier qui contient directement les CSV :

```powershell
python -m pip install -r .\ingestion\requirements.txt
python .\ingestion\replay_history.py 'C:\chemin\vers\PRSA_Data_20130301-20170228'
```

Sur un cluster neuf, le producteur doit confirmer **420 768** messages. Si les 10 premières mesures d'Aotizhongxin ont déjà été envoyées lors du test, ajouter `--skip-test-sample`. Dans ce cas, le producteur doit confirmer **420 758** nouveaux messages et l'historique doit toujours compter **420 768** mesures. En cas d'interruption, vérifier les offsets avant de relancer le producteur, car une nouvelle lecture des mêmes CSV publie de nouveaux messages.

## Vérifier et transmettre les données

```powershell
docker compose -f infra/docker-compose.yml exec namenode hdfs dfs -ls /airwatch/observations
docker compose -f infra/docker-compose.yml exec -w /tmp spark-master /opt/spark/bin/spark-sql --master spark://spark-master:7077 --conf spark.driver.host=spark-master --conf spark.cores.max=1 --conf spark.sql.shuffle.partitions=8 -e 'SELECT COUNT(*) AS nb_mesures, COUNT(DISTINCT station, event_time) AS nb_distinctes, COUNT(DISTINCT station) AS nb_stations FROM parquet.`hdfs://namenode:9000/airwatch/observations`'
```

Lorsque Spark a terminé de consommer Kafka, `nb_mesures` et `nb_distinctes` doivent valoir **420 768** et `nb_stations` doit valoir **12**. Pour le test initial des 10 premières lignes d'Aotizhongxin, une lecture Spark SQL a renvoyé **10 mesures** et une moyenne PM2.5 de **4,50 µg/m³**.

Le traitement batch peut lire `hdfs://namenode:9000/airwatch/observations`. Les colonnes `station` et `event_time` identifient la mesure, `pm25` contient la concentration observée en µg/m³ et `event_date` permet de filtrer les partitions.

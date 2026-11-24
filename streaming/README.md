# Streaming : prédiction de PM2.5 à t+1 h en temps réel

Le job `stream_predict.py` lit les mesures dans Kafka, calcule les valeurs passées de chaque station, applique un modèle Spark ML et écrit les prédictions dans PostgreSQL.

## Fonctionnement
1. **Lecture** du topic Kafka avec Spark Structured Streaming.
2. **Décodage** du JSON et reconstitution de `event_time`.
3. **Lags par station** : `applyInPandasWithState` garde en état les 3 dernières valeurs de PM2.5 de chaque station. L'état est enregistré dans le checkpoint HDFS.
4. **Prédiction** : un `PipelineModel` chargé une seule fois au démarrage prédit PM2.5 pour l'heure suivante.
5. **Écriture** de chaque lot dans la table `predictions` (PostgreSQL), avec le délai mesuré (`delai_s`).

## Contrat du modèle
- Chemin : `hdfs://namenode:9000/airwatch/models/provisional_lr` (variable `MODEL_PATH`).
- Colonnes d'entrée : `pm25`, `pm25_lag1`, `pm25_lag2`, `pm25_lag3`, `PM10`, `NO2`, `CO`, `O3`, `TEMP`, `PRES`, `WSPM`, `hour`, `month`.
- `prediction` = PM2.5 de l'heure suivante, en µg/m³.
- Une mesure dont une variable manque n'est pas prédite (le `VectorAssembler` l'écarte).

## Prérequis
L'image Spark n'inclut pas les bibliothèques Python nécessaires. Dans les 3 conteneurs Spark :
```powershell
foreach ($s in "spark-master","spark-worker-1","spark-worker-2") { docker compose -f infra/docker-compose.yml exec -u root $s pip3 install numpy pandas pyarrow }
```
Cette installation disparaît si les conteneurs sont recréés.

## Lancement
```powershell
docker compose -f infra/docker-compose.yml up -d postgres
docker compose -f infra/docker-compose.yml cp streaming/stream_predict.py spark-master:/tmp/stream_predict.py
docker compose -f infra/docker-compose.yml exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.driver.host=spark-master --conf spark.cores.max=1 --conf spark.hadoop.dfs.replication=2 --conf spark.sql.shuffle.partitions=2 --conf spark.jars.ivy=/tmp/airwatch-ivy --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.7.4 /tmp/stream_predict.py
```
Envoi de mesures simulées (une par seconde) :
```powershell
py -3.12 .\ingestion\stream_replay.py 'CHEMIN\PRSA_Data_Changping_20130301-20170228.csv' 200
```

## Résultats mesurés
| Mesure | Valeur |
|---|---|
| Délai moyen Kafka → traitement | [1,3 s avec Parquet ; à remplacer par la mesure PostgreSQL] |
| Délai maximal en fonctionnement normal | [3,64 s] |
| Partitions d'état | 2 (200 par défaut : lots de 26 s au lieu de 1 à 2 s) |

## Test de reprise
200 mesures de `Changping` envoyées, job arrêté vers la 80e, redémarré avec le même checkpoint après la fin de l'envoi.
- Prédictions attendues sans panne (calculées sur le CSV avec `check_expected.py`) : **192**.
- Prédictions obtenues : **192**, aucun doublon.
- Les 5 mesures écartées ont une valeur manquante dans le CSV.
- Délai maximal après la panne (durée de l'arrêt + rattrapage) : **264 s**.

## Limites connues
- Si des mesures sont rejouées ou arrivent dans le désordre, les lags d'une station peuvent être faux.
- Les heures dans PostgreSQL sont en UTC (8 h de moins qu'à Pékin).
- Le modèle actuel est provisoire (régression linéaire) : il sera remplacé par celui de l'équipe ML, en changeant `MODEL_PATH`.
- Le mot de passe PostgreSQL du compose est un mot de passe de démonstration.
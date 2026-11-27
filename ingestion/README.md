# AirWatch — Mise en place de l'infrastructure et pipeline d'ingestion

## 1. Mise en place de l'infrastructure

La première étape consiste à mettre en place l'environnement distribué à l'aide de Docker Compose, à travers le fichier `infra/docker-compose.yml`.

L'infrastructure comprend deux clusters :

**Cluster Spark :**
- 1 Master
- 2 Workers

**Cluster HDFS :**
- 1 NameNode
- 2 DataNodes
- Facteur de réplication : 2

### Commandes utilisées

Vérification de la configuration Docker Compose :

```powershell
docker compose -f infra/docker-compose.yml config
```

Démarrage des conteneurs :

```powershell
docker compose -f infra/docker-compose.yml up -d
```

Vérification des conteneurs actifs :

```powershell
docker ps
```

Vérification de l'état des DataNodes :

```powershell
docker exec airwatch-namenode hdfs dfsadmin -report
```

## 2. Préparation des données

Les 12 fichiers CSV du dataset ont été placés dans le dossier `data/`, situé à la racine du projet.

Ces fichiers constituent la source de données qui sera utilisée par le Producer Python pour alimenter Kafka.

## 3. Configuration de l'ingestion et création du topic Kafka

### Fichier `config.py`

Le fichier `ingestion/config.py` permet de centraliser les paramètres nécessaires à l'ingestion :

- Le chemin d'accès au dossier contenant les fichiers CSV.
- L'adresse du broker Kafka : `localhost:9092`.
- Le nom du topic : `air-quality`.
- Les paramètres complémentaires de l'ingestion.

### Création du topic Kafka

Un topic nommé `air-quality` a été créé dans Kafka avec trois partitions et un facteur de réplication de 1.

Commande de création :

```bash
kafka-topics.sh --create \
  --topic air-quality \
  --bootstrap-server kafka:29092 \
  --partitions 3 \
  --replication-factor 1
```

Vérification du topic :

```bash
kafka-topics.sh --describe \
  --topic air-quality \
  --bootstrap-server kafka:29092
```

## 4. Développement du Producer

Le fichier `ingestion/producer.py` permet de lire les fichiers CSV et de publier les observations dans Kafka.

Son fonctionnement repose sur les opérations suivantes :

1. Lecture des 12 fichiers CSV à l'aide de Pandas.
2. Identification de la station à partir du nom de chaque fichier.
3. Ajout d'une colonne `station` aux observations.
4. Regroupement des observations dans un DataFrame.
5. Parcours des observations une par une.
6. Conversion de chaque observation en dictionnaire Python.
7. Remplacement des valeurs manquantes par `None`.
8. Sérialisation des observations au format JSON.
9. Envoi des messages au broker Kafka.

Le Producer utilise également `acks="all"` et `retries=5` pour renforcer la fiabilité de l'envoi des messages.

### Chaîne d'ingestion

Le script `producer.py` lit les fichiers CSV, récupère les observations et les envoie sous forme de messages au broker Kafka. Le broker reçoit ensuite ces messages et les conserve dans le topic `air-quality`.

```text
┌──────────────────┐
│    Fichiers CSV  │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Producer Python │
│   producer.py    │
│ Lecture et       │
│ récupération des │
│ observations     │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│   Broker Kafka   │
│ Réception des    │
│ messages         │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Topic air-quality│
│ Conservation des │
│ messages dans    │
│ 3 partitions     │
└──────────────────┘
```

### Exécution du Producer

Depuis la racine du projet :

```powershell
python ingestion/producer.py
```

## 5. Publication et conservation des messages dans Kafka

Après leur lecture et leur conversion au format JSON, les observations sont envoyées par le Producer au broker Kafka.

Le broker reçoit les messages et les conserve dans le topic `air-quality`, réparti en trois partitions.

Cette étape permet de séparer la production des données de leur consommation par les traitements en aval.

## 6. Transfert des données de Kafka vers HDFS

Le script `kafka_to_hdfs.py` utilise Spark Structured Streaming pour consommer les messages du topic Kafka et les transférer vers HDFS.

Le traitement consiste à :

1. Se connecter au broker Kafka via `kafka:29092`.
2. S'abonner au topic `air-quality`.
3. Lire les messages à partir des premiers offsets disponibles avec `startingOffsets = earliest`.
4. Convertir les valeurs des messages en texte.
5. Écrire les données dans HDFS au format texte.
6. Utiliser un dossier de checkpoint pour suivre la progression du traitement.

Le transfert est effectué par micro-batchs avec un intervalle de 10 secondes.

**Chemin de stockage dans HDFS :**

```text
/airwatch/raw/air-quality
```

**Chemin du checkpoint :**

```text
/airwatch/checkpoints/kafka-to-hdfs
```

## 7. Nature de l'ingestion : Batch ou Streaming ?

Dans notre implémentation, les données sources sont historiques et déjà disponibles sous forme de fichiers CSV.

Le Producer lit ces fichiers et publie leurs observations dans Kafka. Cette première étape correspond donc à une ingestion de données historiques par batch/rejeu.

En revanche, la consommation des messages depuis Kafka par Spark Structured Streaming est réalisée en mode streaming, selon un mécanisme de micro-batchs.

Ainsi, notre architecture utilise une approche de streaming alimentée par des données historiques. Il ne s'agit pas d'un flux de données directement reçu depuis des capteurs en temps réel.

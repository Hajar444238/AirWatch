# 🌍 AirWatch — Big Data Platform for Air Quality Monitoring and Prediction

## 1. Présentation du projet

**AirWatch** est une plateforme Big Data dédiée à la surveillance, à l'analyse et à la prévision de la qualité de l'air.

Le projet vise à concevoir une architecture distribuée capable de collecter, stocker et traiter de grands volumes de données environnementales provenant de différentes stations de surveillance.

En exploitant les technologies Big Data, le traitement des flux de données et les techniques de Machine Learning, AirWatch ambitionne de fournir des informations pertinentes sur l'évolution de la pollution atmosphérique et d'anticiper les variations de la concentration des polluants.

## 2. Problématique

La pollution atmosphérique représente un enjeu environnemental et sanitaire majeur. Les stations de surveillance génèrent des volumes importants de données qui nécessitent des mécanismes adaptés pour leur collecte, leur stockage et leur analyse.

Les approches traditionnelles de traitement des données peuvent présenter des limites face à la diversité, au volume et à la fréquence de ces informations.

**Comment concevoir une plateforme Big Data capable de collecter et de traiter efficacement des données environnementales à grande échelle, tout en permettant le suivi de la qualité de l'air et la prévision des niveaux de pollution ?**

## 3. Objectifs du projet

AirWatch poursuit plusieurs objectifs :

- Concevoir et déployer une infrastructure Big Data distribuée et conteneurisée.
- Mettre en place une chaîne d'ingestion des données en utilisant Apache Kafka.
- Assurer le stockage distribué et persistant des données avec Hadoop HDFS.
- Exploiter Apache Spark pour le traitement des données en batch et en streaming.
- Mettre en œuvre des mécanismes de nettoyage, de transformation et de préparation des données.
- Développer des modèles de Machine Learning pour prévoir les concentrations de polluants atmosphériques.
- Mettre à disposition les résultats à travers une base de données et un tableau de bord interactif.

## 4. Dataset

Le projet repose sur le dataset **Beijing Multi-Site Air Quality Data**, disponible sur le dépôt UCI Machine Learning Repository.

Ce jeu de données contient des observations horaires collectées auprès de 12 stations de surveillance de la qualité de l'air à Pékin, entre 2013 et 2017.

**Caractéristiques du dataset :**

| Caractéristique | Description |
|---|---|
| Zone géographique | Pékin, Chine |
| Période | 2013 – 2017 |
| Nombre de stations | 12 |
| Nombre d'observations | 420 768 |
| Fréquence | Horaire |
| Variables | Polluants atmosphériques et données météorologiques |
| Variable cible | PM2.5 |

Les données comprennent notamment les concentrations de PM2.5, PM10, SO2, NO2, CO et O3, ainsi que des variables météorologiques telles que la température, la pression atmosphérique, l'humidité et la vitesse du vent.

Source : [UCI Machine Learning Repository — Beijing Multi-Site Air Quality Data](https://archive.ics.uci.edu/dataset/501/beijing+multi+site+air+quality+data)

## 5. Architecture globale
<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/652e892c-1260-4a3d-8ab7-06c2b041fad5" />

La plateforme AirWatch repose sur une architecture Big Data organisée en plusieurs couches complémentaires.

### 5.1. Couche d'ingestion — Data Ingestion

Cette couche assure la collecte et l'acheminement des données vers l'environnement Big Data.

- Python : lecture et préparation des données sources.
- Apache Kafka : ingestion et transmission des flux de données.
- Kafka Producer : publication des observations dans les topics Kafka.

### 5.2. Couche de stockage — Data Storage

Cette couche assure la conservation des données dans un environnement distribué.

- Hadoop HDFS : stockage distribué et tolérant aux pannes.
- NameNode : gestion des métadonnées du système de fichiers.
- DataNodes : stockage des blocs de données.

### 5.3. Couche de traitement — Data Processing

Cette couche permet de transformer et d'analyser les données collectées.

- Apache Spark : moteur de traitement distribué.
- Spark Batch Processing : traitement des données historiques.
- Spark Structured Streaming : traitement des flux de données en continu.
- Data Cleaning et Data Transformation : préparation et amélioration de la qualité des données.

### 5.4. Couche analytique — Machine Learning

Cette couche exploite les données préparées afin de développer des modèles prédictifs.

- Analyse exploratoire des données.
- Sélection et préparation des variables.
- Entraînement et évaluation des modèles.
- Prévision de la concentration de PM2.5 à partir des observations précédentes.

### 5.5. Couche de restitution — Data Serving & Visualization

Cette couche permet de rendre les résultats accessibles et exploitables.

- Base de données pour la mise à disposition des résultats.
- Tableau de bord interactif pour le suivi des indicateurs de qualité de l'air.
- Visualisation des tendances et des prévisions de pollution.

## 6. Technologies utilisées

| Technologie | Rôle dans le projet |
|---|---|
| Python | Développement des scripts et ingestion |
| Apache Kafka | Gestion des flux de données |
| Apache Spark | Traitement distribué Batch et Streaming |
| Hadoop HDFS | Stockage distribué |
| Docker & Docker Compose | Conteneurisation et orchestration |
| Pandas | Manipulation et préparation des données |
| Machine Learning | Prévision des concentrations de polluants |
| SQL / Base de données | Stockage et consultation des résultats |
| Outil de visualisation | Création du tableau de bord |

## 7. Architecture technique cible

Le flux global de données de la plateforme est organisé comme suit :

**Dataset UCI → Python Producer → Apache Kafka → Spark Structured Streaming → HDFS → Spark Processing → Machine Learning → Base de données → Dashboard**

En parallèle, les données historiques peuvent être exploitées par des traitements Batch avec Apache Spark.

L'ensemble des composants distribués est déployé dans un environnement conteneurisé avec Docker.

## 8. Résultats attendus

La plateforme AirWatch a pour ambition de fournir :

- Une infrastructure Big Data distribuée et évolutive.
- Une chaîne d'ingestion capable de gérer des flux de données environnementales.
- Un système de stockage et de traitement adapté aux volumes importants de données.
- Des analyses permettant de comprendre l'évolution de la pollution atmosphérique.
- Un mécanisme de prévision des concentrations de PM2.5.
- Un tableau de bord facilitant la consultation des indicateurs et des résultats prédictifs.

## 9. Organisation du projet

Le développement de la plateforme est organisé autour de plusieurs modules :

- **Infrastructure :** déploiement et configuration du cluster Big Data.
- **Ingestion :** collecte et transmission des données avec Kafka.
- **Stockage :** organisation et conservation des données avec HDFS.
- **Traitement :** traitement Batch et Streaming avec Spark.
- **Machine Learning :** développement et évaluation des modèles prédictifs.
- **Restitution :** stockage des résultats et visualisation à travers un dashboard.

## 10. Auteurs

**Projet académique Big Data — AirWatch**

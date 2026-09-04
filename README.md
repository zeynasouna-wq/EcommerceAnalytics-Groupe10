# EcommerceAnalytics

Système d'analyse de données e-commerce distribué — Projet final Spark & Scala (Groupe 3).

Pipeline Spark en Scala qui ingère des données transactionnelles multi-format
(CSV, JSON, Parquet), les valide, les enrichit, puis produit des indicateurs
métier (KPI marchands, cohortes, segmentation, etc.).

## Prérequis

| Outil | Version utilisée par le projet | Installation |
|---|---|---|
| **Scala**   | 2.12.18 | Fournie par SBT (`sbt` télécharge le bon compilateur automatiquement) |
| **Apache Spark** | 3.5.1 | [Télécharger Spark](https://spark.apache.org/downloads.html) et suivre les instructions d'installation pour votre OS. Vérifiez avec `spark-submit --version`. |
| **SBT** (Simple Build Tool) | 1.9.9 | [Instructions officielles d'installation](https://www.scala-sbt.org/download.html) (SBT installe lui-même la bonne version de Scala au premier lancement). Vérifiez avec `sbt sbtVersion`. |
| **Java (JDK)** | 8 ou 11 (recommandé par Spark 3.5.x) | `java -version` |

Le fichier `project/build.properties` fixe la version de SBT (`sbt.version=1.9.9`) :
aucune installation manuelle de Scala n'est nécessaire, SBT s'en occupe.

## Structure du projet

```
EcommerceAnalytics/
├── build.sbt
├── README.md
├── EQUIPE.md
├── CONTRIBUTIONS.md
├── .gitignore
└── src/
    ├── main/scala/com/ecommerce/
    │   ├── analytics/     # Ingestion, validation, transformation, analytics, main
    │   ├── models/        # Case classes (Transaction, User, Product, Merchant)
    │   └── utils/         # ConfigLoader, SparkSessionBuilder
    └── main/resources/
        ├── application.conf
        └── data/           # transactions.csv, users.json, products.parquet, merchants.csv
```

## Configuration

Tous les paramètres (chemins des fichiers de données, master Spark, seuils de
validation, options d'optimisation) sont externalisés dans
`src/main/resources/application.conf`. Aucun chemin ni seuil n'est codé en dur
dans le code Scala — pour changer d'environnement (données, cluster, seuils
métier), il suffit de modifier ce fichier.

Avant la première exécution, placez les 4 jeux de données dans
`src/main/resources/data/` (`transactions.csv`, `users.json`,
`products.parquet`, `merchants.csv`), ou adaptez les chemins dans
`application.conf` si vos données se trouvent ailleurs.

## Compilation

Depuis la racine du projet (`EcommerceAnalytics/`) :

```bash
# Compiler le projet et vérifier qu'il n'y a pas d'erreur
sbt compile

# Générer le JAR exécutable (via sbt-assembly)
sbt assembly
```

Le JAR généré se trouve dans `target/scala-2.12/EcommerceAnalytics.jar`.
Spark n'est pas embarqué dans ce JAR (dépendance en `Provided` dans
`build.sbt`) : il est fourni par l'environnement d'exécution
(SBT en local, ou le cluster via `spark-submit`).

## Exécution locale (avec SBT)

Pour lancer le pipeline complet directement avec SBT, sans passer par
`spark-submit` (pratique en développement) :

```bash
sbt run
```

Le master Spark utilisé en local est défini dans `application.conf`
(`app.spark.master = "local[*]"` par défaut, ce qui utilise tous les cœurs
disponibles de la machine).

## Déploiement (spark-submit)

Une fois le JAR généré (`sbt assembly`), il peut être exécuté sur un cluster
Spark (ou en local) avec :

```bash
spark-submit \
  --class com.ecommerce.analytics.MainApp \
  --master local[*] \
  target/scala-2.12/EcommerceAnalytics.jar
```

Pour un déploiement sur un vrai cluster (YARN, standalone, etc.), remplacez
`--master local[*]` par l'adresse du cluster cible, par exemple :

```bash
spark-submit \
  --class com.ecommerce.analytics.MainApp \
  --master spark://<host-du-master>:7077 \
  target/scala-2.12/EcommerceAnalytics.jar
```

## Résultats produits

Les résultats sont écrits dans le répertoire défini par
`app.data.output.path` (`output/` par défaut), notamment :

- `output/data_quality_report/` — rapport de qualité des données (CSV), une
  ligne par dataset avec le nombre de lignes lues/valides/rejetées, le taux
  de rejet et le nombre de valeurs nulles détectées.

## Comparatif des performances (optimisations Spark)

*À compléter par le Membre C (Question 5.3, bonus) avec les temps d'exécution
mesurés avant/après activation du cache et du broadcast.*

| Étape | Durée sans optimisation | Durée avec optimisation | Gain |
|---|---|---|---|
| Ingestion | — | — | — |
| Transformation | — | — | — |
| Analytique | — | — | — |
| Écriture | — | — | — |

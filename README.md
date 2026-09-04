# EcommerceAnalytics

Système d'analyse de données e-commerce distribué — Projet final Spark & Scala (Groupe 10).

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


## Workflow depuis le clonage du projet

Après avoir cloné le dépôt, suivre les étapes suivantes depuis la racine du projet :

```bash
# 1. Cloner le dépôt
git clone <URL_DU_DEPOT>

# 2. Se placer dans le projet
cd EcommerceAnalytics

# 3. Vérifier la version de SBT
sbt sbtVersion

# 4. Vérifier la version de Java
java -version

# 5. Nettoyer le projet
sbt clean

# 6. Compiler le projet
sbt compile

# 7. Exécuter le pipeline complet pour les tests
sbt "runMain com.ecommerce.analytics.MainApp all"

# 8. Générer le JAR
sbt assembly

# 9. Tester les différents modes avec spark-submit
spark-submit --class com.ecommerce.analytics.MainApp app.jar all
spark-submit --class com.ecommerce.analytics.MainApp app.jar ingestion
spark-submit --class com.ecommerce.analytics.MainApp app.jar validation
spark-submit --class com.ecommerce.analytics.MainApp app.jar analytics
spark-submit --class com.ecommerce.analytics.MainApp app.jar comparaison
```

> **Remarque :** avant l'exécution, vérifier que les jeux de données sont présents
> dans `src/main/resources/data/` et que la configuration de
> `src/main/resources/application.conf` correspond à l'environnement utilisé.

## Exécution locale (avec SBT)

Pour lancer le pipeline complet directement avec SBT, sans passer par
`spark-submit` (pratique en développement) :

```bash
sbt run
```

Le master Spark utilisé en local est défini dans `application.conf`
(`app.spark.master = "local[*]"` par défaut, ce qui utilise tous les cœurs
disponibles de la machine).


## Commandes des différents modes d'exécution

Les différents traitements peuvent être lancés séparément avec `runMain` :

```bash
# Pipeline complet
sbt "runMain com.ecommerce.analytics.MainApp all"

# Ingestion
sbt "runMain com.ecommerce.analytics.MainApp ingestion"

# Validation
sbt "runMain com.ecommerce.analytics.MainApp validation"

# Analytique
sbt "runMain com.ecommerce.analytics.MainApp analytics"

# Comparaison des performances - Question 5.3
sbt "runMain com.ecommerce.analytics.MainApp comparaison"
```

## Déploiement (spark-submit)

Une fois le JAR généré (`sbt assembly`), il peut être exécuté sur un cluster
Spark (ou en local) avec :

```bash
spark-submit \
  --class com.ecommerce.analytics.MainApp \
  --master local[*] \
  target/scala-2.12/EcommerceAnalytics.jar
```


### Commande indiquée dans l'épreuve

Pour reproduire l'exécution demandée dans l'épreuve :

```bash
spark-submit --class com.ecommerce.analytics.MainApp app.jar all
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

| Étape          | Durée sans optimisation | Durée avec optimisation | Gain        |
|----------------|------------------------:|------------------------:|------------:|
| Ingestion      | 5,448 s                 | 1,001 s                 | 81,63 %     |
| Transformation | 1,376 s                 | 1,877 s                 | -36,42 %    |
| Analytique     | 2,692 s                 | 1,196 s                 | 55,59 %     |
| Écriture       | 4,214 s                 | 2,208 s                 | 47,60 %     |
| **TOTAL**      | **13,729 s**            | **6,281 s**             | **54,25 %** |

Optimisations utilisées dans la seconde exécution :

- `broadcast()` des petites tables ;
- `cache()` du DataFrame enrichi ;
- configuration de `spark.sql.shuffle.partitions`.

Formule du gain :

```text
Gain (%) = ((Temps sans optimisation - Temps avec optimisation)
            / Temps sans optimisation) × 100
```

Le temps total mesuré passe de **13,729 secondes** sans optimisation à
**6,281 secondes** avec optimisation, soit un gain global de **54,25 %**.

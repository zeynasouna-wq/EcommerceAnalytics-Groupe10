# Journal de contribution — Groupe 10

## Tableau récapitulatif : question → responsable → relecteur

| Partie / Question | Responsable | Relecteur | Statut |
|---|---|---|---|
| 0.1 — Constitution du groupe | Collectif | — | Fait |
| 0.2 — Suivi Git | Collectif | — | Fait - Dépôt Git local + GitHub créés |
| 0.3 — Journal de contribution | Collectif | — | Fait |
| 1.1 — Structure SBT | Membre A | Membre C | Fait |
| 1.2 — build.sbt | Membre A | Membre C | Fait |
| 1.3 — README.md | Membre A | Membre C | Fait |
| 2.1 — Ingestion multi-format | Membre A | Membre B | Fait |
| 2.2 — Validation des données | Membre A | Membre B | Fait |
| 2.3 — Gestion d'erreurs et résumé | Membre A | Membre B | Fait |
| 2.4 — Rapport de qualité des données | Membre A | Membre B | Fait |
| 2.5 — Intégrité référentielle (bonus) | Membre A | Membre B | Fait |
| 3.1 — UDF extractTimeFeatures | Membre B | Membre A | Fait |
| 3.2 — enrichTransactionData | Membre B | Membre A | Fait |
| 3.3 — Analyse par partition Window | Membre B | Membre A | Fait |
| 3.4 — Transactions suspectes (bonus) | Membre B | Membre A | Fait |
| 4.1 — Rapport détaillé par marchand | Membre C | Membre B | Fait |
| 4.2 — Analyse de cohortes | Membre C | Membre B | Fait |
| 4.3 — Segmentation RFM (bonus) | Membre C | Membre B | Fait |
| 4.4 — Analyse produits et catégories (bonus) | Membre C | Membre B | Fait |
| 5.1 — Optimisation du stockage | Membre C | Membres A et B | Fait |
| 5.2 — Optimisation des jointures | Membre C | Membres A et B | Fait |
| 5.3 — Mesure du gain (bonus) | Membre C | Membres A et B | Fait |
| 6.1 — EcommerceAnalyticsApp | Membre C | Les 3 membres | Fait |
| 6.2 — Exécution modulaire (bonus) | Membre C | Les 3 membres | Fait |
| 7.1 — application.conf | Membre A | Membre C | Fait |

## Charge de travail estimée par membre

*(À compléter honnêtement par chacun — sert à l'évaluation individuelle.)*

| Membre | Heures estimées | Difficultés rencontrées |
|---|---|---|
| Membre A (Seynabou Souna DIOP) | ~5-6h (< 2h de développement initial + ~4h de débogage environnement, vérification, correction et mise en place Git/GitHub) | Mise en place de l'environnement local sous Windows plus longue que prévu : version de Java trop récente incompatible avec Spark (passage à Java 17), absence de `winutils.exe`/`hadoop.dll` nécessaires à Hadoop sous Windows, erreurs d'accès mémoire liées au système de modules de Java 17 (flags `--add-opens` à ajouter), et un décalage de type entre le JSON inféré par Spark et la case class `User` (`age` en BIGINT vs Int). |
| Membre B (Kemogoha Abdoulaye Coulibaly) | ~8h30 | Difficultés liées à la compatibilité entre Java et Spark : Java 26 provoquait une erreur `UnsupportedOperationException: getSubject is not supported`, résolue avec Java 17. Difficulté également avec les UDF Scala typés dans Spark 3.5.1 (`UNTYPED_SCALA_UDF`), résolue avec l'API Java `UDF1`. Certaines données contenaient aussi des timestamps de 16 caractères au lieu des 14 attendus, provoquant une erreur de parsing. Enfin, la mise en œuvre des fenêtres temporelles et du calcul de la moyenne historique pour la détection des transactions suspectes a nécessité plusieurs tests et ajustements. |
| Membre C (Mahugnon Dieu-Donné Luc DOSSOU KOKO) | ~10h | Configuration de l’exécution Spark avec SBT et Java 17 (ajout des options --add-opens dans build.sbt et utilisation de fork := true pour permettre à Spark de démarrer correctement avec Java 17.); Génération du JAR exécutable avec sbt assembly (configuration de sbt-assembly, définition de MainApp comme mainClass et conservation des dépendances Spark en Provided afin de ne pas intégrer Spark dans le JAR.); Mise en place et vérification de l’exécution avec spark-submit (configuration du projet pour permettre l’exécution avec spark-submit --class com.ecommerce.analytics.MainApp app.jar all et vérification des différents modes d’exécution.) ; Affichage des résultats analytiques trop chargé (utilisation de .select(...) pour afficher uniquement les colonnes pertinentes lors de l’aperçu des données enrichies et des résultats.); Comparaison des performances avec et sans optimisation ( exécution des deux scénarios et mesure séparée des temps d’ingestion, de transformation, d’analytique et d’écriture afin de calculer le gain pour chaque étape et le gain global. Reproductibilité de l’exécution pour les autres membres du groupe (génération d’un JAR exécutable du projet et mise à disposition de celui-ci dans le dépôt, permettant aux autres membres d’utiliser directement la commande spark-submit --class com.ecommerce.analytics.MainApp app.jar all sans devoir reconstruire immédiatement le projet. Les commandes SBT de compilation et de génération du JAR sont également documentées pour permettre de régénérer le JAR en cas de modification.)) |

## Décisions techniques du groupe


1. **Versions Spark/Scala** : Spark 3.5.1 avec Scala 2.12.18. Choix de la dernière
   version stable de la branche 3.5 de Spark, associée à la version de Scala 2.12
   qu'elle supporte officiellement (Spark 3.x n'est pas encore stabilisé sur Scala
   2.13 pour tous les connecteurs utilisés).

2. **Génération du JAR exécutable** : `sbt-assembly` plutôt que `sbt-package`, avec
   les dépendances Spark en scope `Provided`. Cela produit un JAR applicatif léger
   (Spark est fourni par `spark-submit` / le cluster), conforme à l'usage standard
   en production, plutôt qu'un « fat JAR » embarquant inutilement Spark.

3. **Gestion des lignes invalides (Q2.2)** : chaque fonction de validation renvoie
   deux `DataFrame` (valides / rejetées) plutôt que de simplement filtrer les
   lignes invalides. Cela permet de conserver une trace exploitable des rejets
   (colonne `rejection_reason`) au lieu de perdre silencieusement des données.

4. **Comptage des valeurs nulles dans le rapport de qualité (Q2.4)** : calculé sur
   l'ensemble des lignes lues (valides + rejetées recombinées), et non sur les
   seules lignes valides. Sinon, une colonne non couverte par les règles de
   validation (ex : `location`, `city`) verrait ses nulls sous-comptés dès qu'une
   ligne est rejetée pour une autre raison, ce qui fausserait le rapport métier.

5. **Intégrité référentielle (Q2.5, bonus)** : les jointures `left_anti` sont
   effectuées sur les transactions telles que lues, indépendamment du filtrage de
   la Q2.2 (montant/timestamp). Une transaction peut référencer un utilisateur
   inexistant tout en ayant un montant et un timestamp par ailleurs valides : ce
   sont deux dimensions de qualité distinctes qu'il ne faut pas mélanger.

6. **Stratégie de jointure (Q3.2)** : des `LEFT JOIN` sont utilisés entre les transactions et les tables de référence `users`, `products` et `merchants`. Les transactions constituent la donnée principale à conserver : un problème de correspondance dans une table de référence ne doit pas entraîner la suppression de la transaction. Cette stratégie permet également de conserver les lignes présentant une donnée référentielle manquante.

7. **Fenêtres temporelles (Q3.3)** : les analyses cumulées utilisent une fenêtre de 7 jours basée sur le temps (`rangeBetween`) plutôt qu'un nombre fixe de lignes. Cela permet de respecter réellement la notion de période glissante de 7 jours, même lorsque le nombre de transactions varie fortement selon les utilisateurs.

8. **Gestion des timestamps irréguliers (Q3.3)** : avant conversion en `timestamp`, seuls les 14 premiers caractères sont utilisés lorsque les données contiennent des valeurs plus longues que le format attendu. Cette précaution évite qu'une valeur mal formée interrompe l'ensemble du traitement Spark.

9. **Optimisation des jointures (Q5.2)** : utilisation de broadcast() pour les petites tables et configuration du shuffle. Les tables users, products et merchants sont de taille nettement inférieure à transactions. Le choix de broadcast() permet de limiter les échanges liés au shuffle lors des jointures. Le nombre de partitions de shuffle est également configuré via spark.sql.shuffle.partitions afin d'adapter l'exécution aux ressources disponibles.

11. **Mise en cache des données enrichies (Q5.1)** : utilisation de cache() après les jointures. Le DataFrame enrichi est réutilisé par plusieurs traitements analytiques. Le choix de cache() permet de conserver ce résultat en mémoire et d'éviter de recalculer les jointures à chaque utilisation. Le cache est matérialisé avec count() avant les analyses afin de garantir son chargement.

12. **Choix des formats de sortie et organisation du répertoire output/** : les résultats analytiques sont exportés en CSV et Parquet afin de faciliter à la fois leur consultation et leur réutilisation avec Spark. Le répertoire output/ est organisé par traitement, notamment pour distinguer les résultats produits par les analyses et ceux issus de la comparaison des performances.

13. **Segmentation RFM (Q4.3 — bonus)** : utilisation de ntile(5) pour transformer les indicateurs de récence, fréquence et montant en scores de 1 à 5, puis définition de seuils métier sur ces scores pour identifier les segments « Champions », « Clients fidèles », « Nouveaux », « À risque » et « Perdus ». Ce choix permet d'obtenir une segmentation relative aux comportements observés dans le jeu de données et de conserver des règles de classification simples et reproductibles.


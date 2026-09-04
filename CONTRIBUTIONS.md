# Journal de contribution — Groupe 10

## Tableau récapitulatif : question → responsable → relecteur

| Partie / Question | Responsable | Relecteur | Statut |
|---|---|---|---|
| 0.1 — Constitution du groupe | Collectif | — | Fait |
| 0.2 — Suivi Git | Collectif | — | Fait - Dépôt Git local + GitHub créés |
| 0.3 — Journal de contribution | Collectif | — | En cours (ce fichier) |
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
| Membre C | TODO | TODO |

## Décisions techniques du groupe

*(Minimum 5, avec justification en 2-3 lignes chacune. Les 5 premières ci-dessous
correspondent à des choix déjà faits dans le code du Membre A — à compléter avec
les décisions prises par les Membres B et C au fur et à mesure de leur avancement,
par exemple : stratégie de jointure retenue en Partie 3, format de sortie en
Partie 6, seuils de la segmentation RFM en Partie 4.3.)*

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

9. TODO (Membre C — Partie 6) : format(s) de sortie retenu(s) pour les résultats
   finaux (CSV et/ou Parquet) et organisation du répertoire `output/`.


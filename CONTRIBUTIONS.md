# Journal de contribution — Groupe 10

## Tableau récapitulatif : question → responsable → relecteur

| Partie / Question | Responsable | Relecteur | Statut |
|---|---|---|---|
| 0.1 — Constitution du groupe | Collectif | — | Partiel - Membre A renseignée, B et C à ajouter |
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
| 3.1 — UDF extractTimeFeatures | Membre B | Membre A | À faire |
| 3.2 — enrichTransactionData | Membre B | Membre A | À faire |
| 3.3 — Analyse par partition Window | Membre B | Membre A | À faire |
| 3.4 — Transactions suspectes (bonus) | Membre B | Membre A | À faire |
| 4.1 — Rapport détaillé par marchand | Membre C | Membre B | À faire |
| 4.2 — Analyse de cohortes | Membre C | Membre B | À faire |
| 4.3 — Segmentation RFM (bonus) | Membre C | Membre B | À faire |
| 4.4 — Analyse produits et catégories (bonus) | Membre C | Membre B | À faire |
| 5.1 — Optimisation du stockage | Membre C | Membres A et B | À faire |
| 5.2 — Optimisation des jointures | Membre C | Membres A et B | À faire |
| 5.3 — Mesure du gain (bonus) | Membre C | Membres A et B | À faire |
| 6.1 — EcommerceAnalyticsApp | Membre C | Les 3 membres | À faire |
| 6.2 — Exécution modulaire (bonus) | Membre C | Les 3 membres | À faire |
| 7.1 — application.conf | Membre A | Membre C | Fait |

## Charge de travail estimée par membre

*(À compléter honnêtement par chacun — sert à l'évaluation individuelle.)*

| Membre | Heures estimées | Difficultés rencontrées |
|---|---|---|
| Membre A (Seynabou Souna DIOP) | ~5-6h (< 2h de développement initial + ~4h de débogage environnement, vérification, correction et mise en place Git/GitHub) | Mise en place de l'environnement local sous Windows plus longue que prévu : version de Java trop récente incompatible avec Spark (passage à Java 17), absence de `winutils.exe`/`hadoop.dll` nécessaires à Hadoop sous Windows, erreurs d'accès mémoire liées au système de modules de Java 17 (flags `--add-opens` à ajouter), et un décalage de type entre le JSON inféré par Spark et la case class `User` (`age` en BIGINT vs Int). |
| Membre B | TODO | TODO |
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

6. TODO (Membre B — Partie 3) : type de jointure retenu pour combiner transactions
   / users / products / merchants (inner, left, etc.) et justification.

7. TODO (Membre C — Partie 6) : format(s) de sortie retenu(s) pour les résultats
   finaux (CSV et/ou Parquet) et organisation du répertoire `output/`.

## Relectures croisées

*(Chaque module doit être relu par un autre membre que son auteur. Ajouter une
ligne datée par relecture effectuée.)*

| Date | Module relu | Relecteur | Remarques |
|---|---|---|---|
| TODO | TODO | TODO | TODO |

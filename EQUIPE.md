# Équipe — Groupe 10

## Membres

| Nom | Prénom | E-mail | `git config user.name` |
|---|---|---|---|
| DIOP | Seynabou Souna | zeynasouna@gmail.com | Seynabou Souna DIOP |
| COULIBALY | Kemogoha Abdoulaye | abdoullahcoulibaly2@gmail.com | Kemogoha Abdoulaye Coulibaly |
| DOSSOU KOKO  | Mahugnon Dieu-Donné Luc | dossou10luc@gmail.com | dossou10luc (Mahugnon Dieu-Donné Luc DOSSOU KOKO) |


> Chaque membre doit exécuter sur son propre poste, avant le premier commit :
> ```bash
> git config user.name "Prénom Nom"
> git config user.email "adresse@mail"
> ```
> Le nom renseigné ci-dessus doit correspondre exactement à celui utilisé dans
> `git config user.name`, afin que `git log` permette d'identifier qui a fait quoi.

## Rôles et périmètres

| Rôle | Membre | Périmètre principal | Livrables dont le membre est propriétaire |
|---|---|---|---|
| **Membre A** — Data Ingestion & Platform Engineer | Seynabou Souna DIOP | Parties 1, 2 et 7 | Structure SBT, `build.sbt`, case classes, `DataIngestion.scala`, `DataValidation.scala`, `DataQualityReport.scala`, `application.conf`, `README.md` |
| **Membre B** — Data Transformation Engineer | Kemogoha Abdoulaye Coulibaly | Partie 3 | UDF `extractTimeFeatures`, `DataTransformation.scala`, jointures d'enrichissement, fonctions de fenêtrage, détection de comportements |
| **Membre C** — Analytics & Performance Engineer | Mahugnon Dieu-Donné Luc DOSSOU KOKO| Parties 4, 5 et 6 | `Analytics.scala`, `SparkOptimization.scala`, KPI marchands, cohortes, segmentation RFM, optimisations Spark, `MainApp.scala`, écriture des résultats |


Les Parties 8 et 9 (tests, qualité, documentation, soutenance) sont réalisées
collectivement : chaque membre y contribue pour la portion du code dont il
est propriétaire.

## Liste nominative des questions traitées

*(à compléter au fur et à mesure — sert de base au tableau croisé de CONTRIBUTIONS.md)*

| Membre | Questions traitées |
|---|---|
| Membre A | 0.1 / 0.2 / 0.3 (contribution collective), 1.1, 1.2, 1.3, 2.1, 2.2, 2.3, 2.4, 2.5 (bonus), 7.1 |
| Membre B | 3.1 / 3.2 / 3.3 / 3.4 (bonus) |
| Membre C | 4.1 / 4.2/ 4.3 (bonus) / 4.4 (bonus) / 5.1 / 5.2 / 5.3 (bonus) / 6.1 / 6.2 (bonus)|


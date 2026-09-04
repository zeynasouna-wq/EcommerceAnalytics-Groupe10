package com.ecommerce.analytics

import com.typesafe.config.Config

import org.apache.spark.sql.{
  DataFrame,
  SparkSession
}

import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel


/**
 * ============================================================
 * OPTIMISATIONS SPARK - MEMBRE C
 * ============================================================
 *
 * QUESTION 5 :
 *
 * 5.1 - Cache / Persist / Unpersist
 * 5.2 - Broadcast Join / Shuffle Partitions
 * 5.3 - Comparaison avant / après optimisation
 *
 * Ce module regroupe toutes les fonctions d'optimisation
 * utilisées par MainApp.
 */
object SparkOptimizations {


  // ============================================================
  // QUESTION 5.1 - CACHE
  // ============================================================

  /**
   * Met un DataFrame en cache mémoire.
   *
   * Le cache est particulièrement utile lorsqu'un même
   * DataFrame est utilisé plusieurs fois.
   *
   * Le count() matérialise réellement le cache.
   */
  def cacheDataFrame(
      df: DataFrame
  ): DataFrame = {

    val cached =
      df.cache()

    // ----------------------------------------------------------
    // Matérialisation du cache
    // ----------------------------------------------------------

    cached.count()

    cached
  }


  // ============================================================
  // QUESTION 5.1 - PERSIST
  // ============================================================

  /**
   * Persiste un DataFrame avec MEMORY_AND_DISK_SER.
   *
   * Les données sont conservées en mémoire lorsque cela est
   * possible et peuvent être stockées sur disque si nécessaire.
   */
  def persistDataFrame(
      df: DataFrame
  ): DataFrame = {

    val persisted =
      df.persist(
        StorageLevel.MEMORY_AND_DISK_SER
      )

    // ----------------------------------------------------------
    // Matérialisation du stockage
    // ----------------------------------------------------------

    persisted.count()

    persisted
  }


  // ============================================================
  // QUESTION 5.1 - UNPERSIST
  // ============================================================

  /**
   * Libère les données précédemment mises en cache ou persistées.
   */
  def unpersistDataFrame(
      df: DataFrame
  ): Unit = {

    df.unpersist(
      blocking = false
    )
  }


  // ============================================================
  // QUESTION 5.2 - SHUFFLE PARTITIONS
  // ============================================================

  /**
   * Configure le nombre de partitions utilisées lors
   * des opérations de shuffle.
   *
   * La valeur est récupérée depuis application.conf :
   *
   * app.spark.shuffle.partitions
   */
  def configureShufflePartitions(
      spark: SparkSession,
      config: Config
  ): Unit = {

    val shufflePartitions =
      config.getInt(
        "app.spark.shuffle.partitions"
      )

    spark.conf.set(
      "spark.sql.shuffle.partitions",
      shufflePartitions
    )

    println(
      s"spark.sql.shuffle.partitions = $shufflePartitions"
    )
  }


  // ============================================================
  // QUESTION 5.2 - BROADCAST JOIN
  // ============================================================

  /**
   * Effectue une jointure avec broadcast.
   *
   * Le broadcast est adapté lorsqu'une table est suffisamment
   * petite pour être envoyée aux différents exécutors.
   */
  def broadcastMerchantJoin(
      transactions: DataFrame,
      merchants: DataFrame
  ): DataFrame = {

    transactions.join(
      broadcast(merchants),
      Seq("merchant_id"),
      "left"
    )
  }


  // ============================================================
  // QUESTION 5.2 - ENRICHISSEMENT OPTIMISÉ
  // ============================================================

  /**
   * Effectue l'enrichissement des transactions avec
   * les tables de référence.
   *
   * Optimisations appliquées :
   *
   * 1. broadcast(users)
   * 2. broadcast(products)
   * 3. broadcast(merchants)
   *
   * Les colonnes potentiellement dupliquées sont renommées
   * avant les jointures afin d'éviter les ambiguïtés.
   *
   * Les caractéristiques temporelles sont calculées à l'aide
   * du UDF défini dans TimeFeatures.scala.
   */
  def optimizedEnrichTransactionData(
      transactions: DataFrame,
      users: DataFrame,
      products: DataFrame,
      merchants: DataFrame
  ): DataFrame = {

    // ==========================================================
    // PREPARATION DE USERS
    // ==========================================================

    /*
     * merchant_id provenant éventuellement de users est supprimé
     * afin d'éviter une colonne dupliquée après les jointures.
     */
    val usersPrepared =
      if (
        users.columns.contains(
          "merchant_id"
        )
      ) {

        users.drop(
          "merchant_id"
        )

      } else {

        users
      }


    // ==========================================================
    // PREPARATION DE PRODUCTS
    // ==========================================================

    /*
     * Les colonnes merchant_id et category sont renommées
     * afin d'éviter les collisions avec les colonnes provenant
     * des autres tables.
     */
    val productsPrepared =
      products
        .withColumnRenamed(
          "merchant_id",
          "product_merchant_id"
        )
        .withColumnRenamed(
          "category",
          "product_category"
        )


    // ==========================================================
    // PREPARATION DE MERCHANTS
    // ==========================================================

    /*
     * name et category sont renommées afin de conserver
     * des noms de colonnes non ambigus.
     */
    val merchantsPrepared =
      merchants
        .withColumnRenamed(
          "name",
          "merchant_name"
        )
        .withColumnRenamed(
          "category",
          "merchant_category"
        )


    // ==========================================================
    // JOINTURES AVEC BROADCAST
    // ==========================================================

    /*
     * transactions constitue la grande table.
     *
     * users, products et merchants sont des tables de référence
     * beaucoup plus petites.
     *
     * broadcast() permet d'éviter un shuffle important
     * de ces petites tables.
     */
    val joinedData =
      transactions
        .join(
          broadcast(
            usersPrepared
          ),
          Seq(
            "user_id"
          ),
          "left"
        )
        .join(
          broadcast(
            productsPrepared
          ),
          Seq(
            "product_id"
          ),
          "left"
        )
        .join(
          broadcast(
            merchantsPrepared
          ),
          Seq(
            "merchant_id"
          ),
          "left"
        )


    // ==========================================================
    // CARACTERISTIQUES TEMPORELLES
    // ==========================================================

    /*
     * Le UDF TimeFeatures attend un String.
     *
     * Le cast explicite évite les problèmes de typage entre
     * Spark Column et le paramètre String attendu par le UDF.
     */
    val withTimeFeatures =
      joinedData
        .withColumn(
          "time_features",
          TimeFeatures.extractTimeFeaturesUDF(
            col(
              "timestamp"
            ).cast(
              "string"
            )
          )
        )


    // ==========================================================
    // EXTRACTION DES CHAMPS DU STRUCT
    // ==========================================================

    /*
     * Le UDF retourne un Struct contenant :
     *
     * hour
     * day_of_week
     * month
     * is_weekend
     * day_period
     * is_working_hours
     *
     * Chaque champ est extrait dans une colonne indépendante.
     */
    val finalData =
      withTimeFeatures
        .withColumn(
          "hour",
          col(
            "time_features.hour"
          )
        )
        .withColumn(
          "day_of_week",
          col(
            "time_features.day_of_week"
          )
        )
        .withColumn(
          "month",
          col(
            "time_features.month"
          )
        )
        .withColumn(
          "is_weekend",
          col(
            "time_features.is_weekend"
          )
        )
        .withColumn(
          "day_period",
          col(
            "time_features.day_period"
          )
        )
        .withColumn(
          "is_working_hours",
          col(
            "time_features.is_working_hours"
          )
        )
        .drop(
          "time_features"
        )


    finalData
  }


  // ============================================================
  // QUESTION 5.3 - MESURE DU TEMPS
  // ============================================================

  /**
   * Mesure le temps d'exécution d'une opération.
   *
   * Le résultat est exprimé en secondes.
   */
  def measureExecutionTime[T](
      operation: => T
  ): (T, Double) = {

    val startTime =
      System.nanoTime()

    val result =
      operation

    val endTime =
      System.nanoTime()

    val elapsedSeconds =
      (endTime - startTime).toDouble /
        1e9

    (
      result,
      elapsedSeconds
    )
  }


  // ============================================================
  // QUESTION 5.3 - STRUCTURE D'UNE MESURE
  // ============================================================

  /**
   * Représente le temps d'exécution d'une étape.
   */
  case class StageTiming(
      stage: String,
      timeSeconds: Double
  )


  // ============================================================
  // QUESTION 5.3 - MESURE D'UNE ETAPE
  // ============================================================

  /**
   * Mesure une étape nommée du pipeline.
   *
   * Les étapes utilisées dans la comparaison sont :
   *
   * - Ingestion
   * - Transformation
   * - Analytique
   * - Ecriture
   */
  def measureStage[T](
      stageName: String,
      operation: => T
  ): (T, StageTiming) = {

    val (
      result,
      elapsedSeconds
    ) =
      measureExecutionTime(
        operation
      )

    val timing =
      StageTiming(
        stageName,
        elapsedSeconds
      )

    println(
      f"[COMPARAISON] $stageName%-20s : $elapsedSeconds%.3f secondes"
    )

    (
      result,
      timing
    )
  }


  // ============================================================
  // QUESTION 5.3 - TEMPS TOTAL
  // ============================================================

  /**
   * Additionne les temps de toutes les étapes.
   */
  def totalExecutionTime(
      timings: Seq[StageTiming]
  ): Double = {

    timings
      .map(
        _.timeSeconds
      )
      .sum
  }


  // ============================================================
  // QUESTION 5.3 - CALCUL DU GAIN
  // ============================================================

  /**
   * Calcule le gain de performance.
   *
   * Formule :
   *
   * Gain (%) =
   *
   * ((temps avant - temps après)
   * / temps avant) * 100
   */
  def calculatePerformanceGain(
      withoutOptimization: Double,
      withOptimization: Double
  ): Double = {

    if (
      withoutOptimization <= 0.0
    ) {

      0.0

    } else {

      (
        (
          withoutOptimization -
            withOptimization
        ) /
          withoutOptimization
      ) * 100.0
    }
  }


  // ============================================================
  // QUESTION 5.3 - COMPARAISON
  // ============================================================

  /**
   * Compare les différentes étapes entre :
   *
   * - l'exécution sans optimisation ;
   * - l'exécution avec optimisation.
   *
   * Les étapes sont comparées indépendamment.
   */
  def comparePerformance(
      withoutOptimization: Seq[StageTiming],
      withOptimization: Seq[StageTiming]
  ): Seq[
    (
      String,
      Double,
      Double,
      Double
    )
  ] = {

    withoutOptimization.map {

      withoutStage =>

        val matchingStage =
          withOptimization.find(
            _.stage ==
              withoutStage.stage
          )

        matchingStage match {

          case Some(
                withStage
              ) =>

            val gain =
              calculatePerformanceGain(
                withoutStage.timeSeconds,
                withStage.timeSeconds
              )

            (
              withoutStage.stage,
              withoutStage.timeSeconds,
              withStage.timeSeconds,
              gain
            )

          case None =>

            (
              withoutStage.stage,
              withoutStage.timeSeconds,
              0.0,
              0.0
            )
        }
    }
  }


  // ============================================================
  // QUESTION 5.3 - TABLEAU DE COMPARAISON
  // ============================================================

  /**
   * Affiche le tableau final demandé pour la question 5.3.
   *
   * Colonnes :
   *
   * Etape
   * Avant optimisation
   * Après optimisation
   * Gain (%)
   *
   * Les quatre étapes doivent apparaître :
   *
   * - Ingestion
   * - Transformation
   * - Analytique
   * - Ecriture
   */
  def printPerformanceComparison(
      comparison: Seq[
        (
          String,
          Double,
          Double,
          Double
        )
      ]
  ): Unit = {

    println()

    println(
      "=========================================================================="
    )

    println(
      "             COMPARAISON DES PERFORMANCES - QUESTION 5.3"
    )

    println(
      "=========================================================================="
    )

    println()

    println(
      f"${"Etape"}%-22s" +
      f"${"Avant (s)"}%-20s" +
      f"${"Apres (s)"}%-20s" +
      f"${"Gain (%)"}%-15s"
    )

    println(
      "--------------------------------------------------------------------------"
    )

    comparison.foreach {

      case (
            stage,
            withoutOptimization,
            withOptimization,
            gain
          ) =>

        println(
          f"$stage%-22s" +
          f"$withoutOptimization%-20.3f" +
          f"$withOptimization%-20.3f" +
          f"$gain%-15.2f"
        )
    }

    println(
      "=========================================================================="
    )
  }


  // ============================================================
  // QUESTION 5.3 - TEMPS TOTAL
  // ============================================================

  /**
   * Affiche le temps total avant et après optimisation
   * ainsi que le gain global.
   */
  def printTotalPerformance(
      withoutOptimization: Seq[StageTiming],
      withOptimization: Seq[StageTiming]
  ): Unit = {

    val totalWithout =
      totalExecutionTime(
        withoutOptimization
      )

    val totalWith =
      totalExecutionTime(
        withOptimization
      )

    val totalGain =
      calculatePerformanceGain(
        totalWithout,
        totalWith
      )

    println()

    println(
      "--------------------------------------------------------------------------"
    )

    println(
      "TEMPS TOTAL DU PIPELINE"
    )

    println(
      "--------------------------------------------------------------------------"
    )

    println(
      f"Avant optimisation : $totalWithout%.3f secondes"
    )

    println(
      f"Apres optimisation : $totalWith%.3f secondes"
    )

    println(
      f"Gain global        : $totalGain%.2f %%"
    )

    println(
      "--------------------------------------------------------------------------"
    )
  }


  // ============================================================
  // QUESTION 5.3 - RESUME
  // ============================================================

  /**
   * Produit une interprétation simple du résultat.
   */
  def printComparisonSummary(
      withoutOptimization: Seq[StageTiming],
      withOptimization: Seq[StageTiming]
  ): Unit = {

    val totalWithout =
      totalExecutionTime(
        withoutOptimization
      )

    val totalWith =
      totalExecutionTime(
        withOptimization
      )

    val gain =
      calculatePerformanceGain(
        totalWithout,
        totalWith
      )

    println()

    println(
      "RESUME DE LA COMPARAISON"
    )

    println(
      "--------------------------------------------------------------------------"
    )

    if (
      gain > 0.0
    ) {

      println(
        f"L'optimisation apporte un gain global de $gain%.2f %%."
      )

    } else if (
      gain < 0.0
    ) {

      println(
        f"L'execution optimisee est plus lente de ${math.abs(gain)}%.2f %%."
      )

    } else {

      println(
        "Aucun gain global mesurable sur cette execution."
      )
    }

    println(
      "--------------------------------------------------------------------------"
    )
  }


  // ============================================================
  // OPTIMISATION COMPLETE
  // ============================================================

  /**
   * Fonction utilitaire regroupant les principales optimisations
   * des questions 5.1 et 5.2.
   *
   * Cette fonction :
   *
   * 1. configure le shuffle ;
   * 2. met les transactions en cache ;
   * 3. effectue une jointure broadcast avec merchants.
   */
  def optimizeTransactions(
      spark: SparkSession,
      config: Config,
      transactions: DataFrame,
      merchants: DataFrame
  ): DataFrame = {

    // ----------------------------------------------------------
    // Configuration du shuffle
    // ----------------------------------------------------------

    configureShufflePartitions(
      spark,
      config
    )

    // ----------------------------------------------------------
    // Mise en cache des transactions
    // ----------------------------------------------------------

    val cachedTransactions =
      cacheDataFrame(
        transactions
      )

    // ----------------------------------------------------------
    // Jointure optimisée
    // ----------------------------------------------------------

    val optimizedJoin =
      broadcastMerchantJoin(
        cachedTransactions,
        merchants
      )

    optimizedJoin
  }
}
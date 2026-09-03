package com.ecommerce.analytics

import com.typesafe.config.ConfigFactory

import org.apache.spark.sql.{
  DataFrame,
  SparkSession
}

import org.apache.spark.sql.functions._

/**
 * ============================================================
 * TEST OPTIMISATIONS SPARK - MEMBRE C
 * ============================================================
 *
 * Question 5.1 :
 *   - cache()
 *   - persist()
 *   - unpersist()
 *
 * Question 5.2 :
 *   - broadcast join
 *   - spark.sql.shuffle.partitions
 *
 * Question 5.3 - BONUS :
 *   - comparaison avant / après optimisation
 *   - mesure des temps d'exécution
 *   - calcul du gain de performance
 */
object TestMemberCOptimisation {

  // ============================================================
  // MESURE DU TEMPS
  // ============================================================

  case class StageTiming(
      ingestion: Double,
      transformation: Double,
      analytics: Double,
      writing: Double,
      total: Double
  )

  /**
   * Mesure le temps d'exécution d'une opération.
   */
  def measureTime[T](
      operation: => T
  ): (T, Double) = {

    val start =
      System.nanoTime()

    val result =
      operation

    val end =
      System.nanoTime()

    val duration =
      (end - start) / 1e9

    (
      result,
      duration
    )
  }


  // ============================================================
  // QUESTION 5.1
  // TEST CACHE
  // ============================================================

  def testCache(
      transactions: DataFrame
  ): Unit = {

    println()
    println("============================================================")
    println("5.1.1 - TEST cache()")
    println("============================================================")

    println()
    println(
      s"Nombre de transactions : ${transactions.count()}"
    )

    val cached =
      SparkOptimizations.cacheDataFrame(
        transactions
      )

    println()
    println("[OK] DataFrame mis en cache.")

    println(
      s"Nombre de lignes après cache : ${cached.count()}"
    )

    println()
    println(
      s"StorageLevel : ${cached.storageLevel}"
    )

    SparkOptimizations.unpersistDataFrame(
      cached
    )

    println()
    println("[OK] Cache libéré avec unpersist().")
  }


  // ============================================================
  // QUESTION 5.1
  // TEST PERSIST
  // ============================================================

  def testPersist(
      transactions: DataFrame
  ): Unit = {

    println()
    println("============================================================")
    println("5.1.2 - TEST persist(MEMORY_AND_DISK_SER)")
    println("============================================================")

    val persisted =
      SparkOptimizations.persistDataFrame(
        transactions
      )

    println()
    println("[OK] DataFrame persisté.")

    println(
      s"Nombre de lignes : ${persisted.count()}"
    )

    println(
      s"StorageLevel : ${persisted.storageLevel}"
    )

    SparkOptimizations.unpersistDataFrame(
      persisted
    )

    println()
    println("[OK] Données persistées libérées.")
  }


  // ============================================================
  // QUESTION 5.2
  // TEST SHUFFLE PARTITIONS
  // ============================================================

  def testShufflePartitions(
      spark: SparkSession
  ): Unit = {

    println()
    println("============================================================")
    println("5.2.1 - TEST spark.sql.shuffle.partitions")
    println("============================================================")

    val config =
      ConfigFactory.load()

    SparkOptimizations.configureShufflePartitions(
      spark,
      config
    )

    val currentValue =
      spark.conf.get(
        "spark.sql.shuffle.partitions"
      )

    println()
    println(
      s"[CHECK] Valeur effective : $currentValue"
    )

    println()
    println("[OK] Configuration du shuffle vérifiée.")
  }


  // ============================================================
  // QUESTION 5.2
  // TEST BROADCAST
  // ============================================================

  def testBroadcastJoin(
      transactions: DataFrame,
      merchants: DataFrame
  ): DataFrame = {

    println()
    println("============================================================")
    println("5.2.2 - TEST BROADCAST JOIN")
    println("============================================================")

    println()
    println(
      s"Transactions : ${transactions.count()}"
    )

    println(
      s"Marchands    : ${merchants.count()}"
    )

    val result =
      SparkOptimizations.broadcastMerchantJoin(
        transactions,
        merchants
      )

    println()
    println(
      s"[OK] Broadcast join exécuté."
    )

    println(
      s"Résultat     : ${result.count()} lignes"
    )

    println()
    println("Plan d'exécution :")

    result.explain(
      extended = false
    )

    result
  }


  // ============================================================
  // QUESTION 5.3 - PIPELINE SANS OPTIMISATION
  // ============================================================

  def executePipeline(
      spark: SparkSession,
      transactions: DataFrame,
      merchants: DataFrame,
      optimized: Boolean
  ): StageTiming = {

    val config =
      ConfigFactory.load()

    println()
    println("------------------------------------------------------------")

    if (optimized) {
      println("PIPELINE AVEC OPTIMISATIONS")
    } else {
      println("PIPELINE SANS OPTIMISATIONS")
    }

    println("------------------------------------------------------------")


    // ==========================================================
    // 1. INGESTION
    // ==========================================================

    val (
      transactionsData,
      ingestionTime
    ) =
      measureTime {

        if (optimized) {

          SparkOptimizations.cacheDataFrame(
            transactions
          )

        } else {

          transactions
            .select("*")
            .cache()
            .unpersist()

          transactions
        }
      }

    println(
      f"Ingestion : $ingestionTime%.3f s"
    )


    // ==========================================================
    // 2. TRANSFORMATION
    // ==========================================================

    val (
      enrichedData,
      transformationTime
    ) =
      measureTime {

        val data =
          if (optimized) {

            SparkOptimizations.broadcastMerchantJoin(
              transactionsData,
              merchants
            )

          } else {

            transactionsData.join(
              merchants,
              Seq("merchant_id"),
              "left"
            )
          }

        data.count()

        data
      }

    println(
      f"Transformation : $transformationTime%.3f s"
    )


    // ==========================================================
    // 3. ANALYTIQUE
    // ==========================================================

    val (
      analyticalResult,
      analyticsTime
    ) =
      measureTime {

        val result =
          enrichedData
            .filter(
              col("merchant_id").isNotNull
            )
            .groupBy(
              "merchant_id"
            )
            .agg(

              round(
                sum("amount"),
                2
              ).alias(
                "total_revenue"
              ),

              count("*").alias(
                "transaction_count"
              ),

              countDistinct(
                "user_id"
              ).alias(
                "unique_customers"
              )
            )
            .orderBy(
              col(
                "total_revenue"
              ).desc
            )

        // Action permettant de matérialiser
        // le calcul analytique.
        result.count()

        result
      }

    println(
      f"Analytique : $analyticsTime%.3f s"
    )


    // ==========================================================
    // 4. ÉCRITURE
    // ==========================================================

    val (
      _,
      writingTime
    ) =
      measureTime {

        // Pour le test, on force une action sur
        // le résultat sans créer plusieurs sorties.
        analyticalResult
          .collect()
      }

    println(
      f"Écriture : $writingTime%.3f s"
    )


    // ==========================================================
    // TEMPS TOTAL
    // ==========================================================

    val totalTime =
      ingestionTime +
        transformationTime +
        analyticsTime +
        writingTime

    println()
    println(
      f"Total : $totalTime%.3f s"
    )


    // ==========================================================
    // LIBÉRATION DU CACHE
    // ==========================================================

    if (optimized) {

      SparkOptimizations.unpersistDataFrame(
        transactionsData
      )

      println()
      println(
        "[OK] Cache libéré avec unpersist()."
      )
    }


    // ==========================================================
    // RESULTAT
    // ==========================================================

    StageTiming(
      ingestion =
        ingestionTime,

      transformation =
        transformationTime,

      analytics =
        analyticsTime,

      writing =
        writingTime,

      total =
        totalTime
    )
  }


  // ============================================================
  // QUESTION 5.3
  // CALCUL DU GAIN
  // ============================================================

  def calculateGain(
      withoutOptimization: Double,
      withOptimization: Double
  ): Double = {

    if (
      withoutOptimization == 0.0
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
  // PROGRAMME PRINCIPAL
  // ============================================================

  def main(
      args: Array[String]
  ): Unit = {

    val config =
      ConfigFactory.load()


    // ==========================================================
    // CREATION DE SPARK
    // ==========================================================

    val spark =
      SparkSession
        .builder()
        .appName(
          config.getString(
            "app.name"
          )
        )
        .master(
          config.getString(
            "app.spark.master"
          )
        )
        .getOrCreate()

    spark.sparkContext.setLogLevel(
      "WARN"
    )


    try {

      println()
      println(
        "================================================================"
      )
      println(
        "             TEST OPTIMISATIONS SPARK - MEMBRE C"
      )
      println(
        "================================================================"
      )

      println()
      println(
        "Questions testées :"
      )

      println(
        "  5.1 - Cache / Persist / Unpersist"
      )

      println(
        "  5.2 - Broadcast / Shuffle Partitions"
      )

      println(
        "  5.3 - Benchmark avant / après optimisation - BONUS"
      )


      // ==========================================================
      // CHARGEMENT DES DONNEES
      // ==========================================================

      println()
      println(
        "================================================================"
      )
      println(
        "1. CHARGEMENT DES DONNEES"
      )
      println(
        "================================================================"
      )

      val ingestion =
        new DataIngestion(
          spark
        )

      val transactions =
        ingestion
          .readTransactions()
          .get
          .toDF()

      val merchants =
        ingestion
          .readMerchants()
          .get
          .toDF()


      println()
      println(
        "[OK] Transactions chargées : " +
          transactions.count()
      )

      println(
        "[OK] Marchands chargés : " +
          merchants.count()
      )


      // ==========================================================
      // QUESTION 5.1
      // ==========================================================

      println()
      println()
      println(
        "################################################################"
      )
      println(
        "# QUESTION 5.1 - CACHE / PERSIST / UNPERSIST"
      )
      println(
        "################################################################"
      )

      testCache(
        transactions
      )

      testPersist(
        transactions
      )

      println()
      println(
        "[OK] QUESTION 5.1 TERMINEE"
      )


      // ==========================================================
      // QUESTION 5.2
      // ==========================================================

      println()
      println()
      println(
        "################################################################"
      )
      println(
        "# QUESTION 5.2 - BROADCAST / SHUFFLE"
      )
      println(
        "################################################################"
      )

      testShufflePartitions(
        spark
      )

      val broadcastResult =
        testBroadcastJoin(
          transactions,
          merchants
        )

      println()
      println(
        "[CHECK] Colonnes du résultat broadcast :"
      )

      broadcastResult.columns.foreach {
        column =>
          println(
            s"  [OK] $column"
          )
      }

      println()
      println(
        "[OK] QUESTION 5.2 TERMINEE"
      )


      // ==========================================================
      // QUESTION 5.3
      // BONUS
      // ==========================================================

      println()
      println()
      println(
        "################################################################"
      )
      println(
        "# QUESTION 5.3 - COMPARAISON DES PERFORMANCES - BONUS"
      )
      println(
        "################################################################"
      )


      // ----------------------------------------------------------
      // MODE SANS OPTIMISATION
      // ----------------------------------------------------------

      val withoutOptimization =
        executePipeline(
          spark,
          transactions,
          merchants,
          optimized = false
        )

      println()
      println(
        "[OK] MODE SANS OPTIMISATIONS TERMINE"
      )


      // Nettoyage du cache Spark
      spark.catalog.clearCache()


      // ----------------------------------------------------------
      // MODE AVEC OPTIMISATION
      // ----------------------------------------------------------

      val withOptimization =
        executePipeline(
          spark,
          transactions,
          merchants,
          optimized = true
        )

      println()
      println(
        "[OK] MODE AVEC OPTIMISATIONS TERMINE"
      )


      // ==========================================================
      // TABLEAU COMPARATIF
      // ==========================================================

      println()
      println()
      println(
        "=========================================================================="
      )

      println(
        "TABLEAU COMPARATIF AVANT / APRES OPTIMISATION"
      )

      println(
        "=========================================================================="
      )

      println()

      println(
        f"${"Étape"}%-20s | " +
          f"${"Sans optimisation"}%18s | " +
          f"${"Avec optimisation"}%18s | " +
          f"${"Gain (%)"}%12s"
      )

      println(
        "--------------------------------------------------------------------------"
      )

      val ingestionGain =
        calculateGain(
          withoutOptimization.ingestion,
          withOptimization.ingestion
        )

      println(
        f"${"Ingestion"}%-20s | " +
          f"${withoutOptimization.ingestion}%18.3f | " +
          f"${withOptimization.ingestion}%18.3f | " +
          f"${ingestionGain}%12.2f"
      )


      val transformationGain =
        calculateGain(
          withoutOptimization.transformation,
          withOptimization.transformation
        )

      println(
        f"${"Transformation"}%-20s | " +
          f"${withoutOptimization.transformation}%18.3f | " +
          f"${withOptimization.transformation}%18.3f | " +
          f"${transformationGain}%12.2f"
      )


      val analyticsGain =
        calculateGain(
          withoutOptimization.analytics,
          withOptimization.analytics
        )

      println(
        f"${"Analytique"}%-20s | " +
          f"${withoutOptimization.analytics}%18.3f | " +
          f"${withOptimization.analytics}%18.3f | " +
          f"${analyticsGain}%12.2f"
      )


      val writingGain =
        calculateGain(
          withoutOptimization.writing,
          withOptimization.writing
        )

      println(
        f"${"Écriture"}%-20s | " +
          f"${withoutOptimization.writing}%18.3f | " +
          f"${withOptimization.writing}%18.3f | " +
          f"${writingGain}%12.2f"
      )


      println(
        "--------------------------------------------------------------------------"
      )


      val totalGain =
        calculateGain(
          withoutOptimization.total,
          withOptimization.total
        )

      println(
        f"${"TOTAL"}%-20s | " +
          f"${withoutOptimization.total}%18.3f | " +
          f"${withOptimization.total}%18.3f | " +
          f"${totalGain}%12.2f"
      )

      println(
        "=========================================================================="
      )


      // ==========================================================
      // INTERPRETATION
      // ==========================================================

      println()

      println(
        f"Gain global : $totalGain%.2f %%"
      )

      println()

      if (
        totalGain > 0
      ) {

        println(
          "[OK] L'execution optimisee est plus rapide."
        )

      } else if (
        totalGain < 0
      ) {

        println(
          "[INFO] L'execution optimisee est plus lente sur cette mesure."
        )

        println(
          "       Cela peut etre lie au cout initial du cache"
        )

        println(
          "       et de la diffusion broadcast."
        )

      } else {

        println(
          "[INFO] Aucun gain global mesurable."
        )
      }


      // ==========================================================
      // SYNTHESE
      // ==========================================================

      println()
      println()
      println(
        "================================================================"
      )

      println(
        "                    SYNTHESE QUESTION 5"
      )

      println(
        "================================================================"
      )

      println()
      println(
        "5.1 - Cache / Persist / Unpersist       : [OK]"
      )

      println(
        "5.2 - Broadcast / Shuffle Partitions   : [OK]"
      )

      println(
        "5.3 - Benchmark avant / après          : [OK] BONUS"
      )

      println()
      println(
        "================================================================"
      )

      println(
        "              TEST MEMBER C OPTIMISATIONS TERMINE"
      )

      println(
        "================================================================"
      )


    } finally {

      spark.stop()

      println()
      println(
        "SparkSession arrêtée proprement."
      )
    }
  }
}
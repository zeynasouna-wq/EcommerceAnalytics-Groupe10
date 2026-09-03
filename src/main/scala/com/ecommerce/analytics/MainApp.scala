package com.ecommerce.analytics

import com.ecommerce.models.{
  Transaction,
  User,
  Product,
  Merchant
}

import com.typesafe.config.{
  Config,
  ConfigFactory
}

import org.apache.spark.sql.{
  DataFrame,
  Dataset,
  SparkSession
}

import org.apache.spark.sql.functions._

import scala.util.Try


/**
 * ============================================================
 * MAIN APPLICATION - ECOMMERCE ANALYTICS
 * ============================================================
 *
 * QUESTION 5 :
 *
 * 5.1 - Optimisation du stockage
 * 5.2 - Optimisation des jointures
 * 5.3 - Comparaison des performances
 *
 * QUESTION 6 :
 *
 * 6.1 - Application principale
 * 6.2 - Exécution modulaire
 *
 * ============================================================
 */
object MainApp {


  // ============================================================
  // 1. CHRONOMETRAGE
  // ============================================================

  /**
   * Mesure le temps d'exécution d'une opération.
   */
  def executeStep[T](
      stepName: String
  )(
      operation: => T
  ): T = {

    val startTime =
      System.nanoTime()

    println()
    println(
      "============================================================"
    )
    println(
      s"DEBUT : $stepName"
    )
    println(
      "============================================================"
    )
    println(
      s"Heure de debut : ${java.time.LocalDateTime.now()}"
    )

    val result =
      operation

    val endTime =
      System.nanoTime()

    val duration =
      (endTime - startTime).toDouble / 1e9

    println()
    println(
      "------------------------------------------------------------"
    )
    println(
      s"FIN : $stepName"
    )
    println(
      "------------------------------------------------------------"
    )
    println(
      s"Heure de fin : ${java.time.LocalDateTime.now()}"
    )
    println(
      f"Duree : $duration%.3f secondes"
    )

    result
  }


  // ============================================================
  // 2. AIDE
  // ============================================================

  def printHelp(): Unit = {

    println()
    println(
      "============================================================"
    )
    println(
      "              MODES D'EXECUTION DISPONIBLES"
    )
    println(
      "============================================================"
    )

    println()

    println("Commandes :")

    println(
      """  sbt "runMain com.ecommerce.analytics.MainApp ingestion""""
    )

    println(
      """  sbt "runMain com.ecommerce.analytics.MainApp transformation""""
    )

    println(
      """  sbt "runMain com.ecommerce.analytics.MainApp analytics""""
    )

    println(
      """  sbt "runMain com.ecommerce.analytics.MainApp comparaison""""
    )

    println(
      """  sbt "runMain com.ecommerce.analytics.MainApp all""""
    )

    println()

    println("Description :")

    println(
      "  ingestion       -> ingestion des quatre sources"
    )

    println(
      "  transformation  -> transformation des donnees validees"
    )

    println(
      "  analytics       -> execution des analyses"
    )

    println(
      "  comparaison     -> comparaison avant/apres optimisation"
    )

    println(
      "  all             -> pipeline complet"
    )

    println()

    println(
      "Sans argument, le mode 'all' est execute par defaut."
    )

    println(
      "============================================================"
    )
  }


  // ============================================================
  // 3. INITIALISATION DE SPARK
  // ============================================================

  def createSparkSession(
      config: Config
  ): SparkSession = {

    val appName =
      Try(
        config.getString(
          "app.name"
        )
      ).getOrElse(
        "EcommerceAnalytics"
      )

    val master =
      Try(
        config.getString(
          "app.spark.master"
        )
      ).getOrElse(
        "local[*]"
      )

    val shufflePartitions =
      Try(
        config.getInt(
          "app.spark.shuffle.partitions"
        )
      ).getOrElse(
        8
      )

    println()
    println(
      "============================================================"
    )
    println(
      "INITIALISATION DE SPARK"
    )
    println(
      "============================================================"
    )
    println(
      s"Application : $appName"
    )
    println(
      s"Master      : $master"
    )
    println(
      s"Shuffle     : $shufflePartitions"
    )

    SparkSession
      .builder()
      .appName(appName)
      .master(master)
      .config(
        "spark.sql.shuffle.partitions",
        shufflePartitions
      )
      
      .getOrCreate()
  }


  // ============================================================
  // 4. CHEMIN DE SORTIE
  // ============================================================

  def getOutputPath(
      config: Config
  ): String = {

    Try(
      config.getString(
        "app.data.output.path"
      )
    ).getOrElse(
      "output/"
    )
  }


  // ============================================================
  // 5. INGESTION
  // ============================================================

  /**
   * Lecture des quatre sources :
   *
   * - transactions
   * - users
   * - products
   * - merchants
   */
  def runIngestion(
      spark: SparkSession
  ): (
      Dataset[Transaction],
      Dataset[User],
      Dataset[Product],
      Dataset[Merchant]
  ) = {

    executeStep(
      "INGESTION"
    ) {

      val ingestion =
        new DataIngestion(
          spark
        )

      val transactions =
        ingestion
          .readTransactions()
          .getOrElse(
            throw new RuntimeException(
              "Impossible de charger transactions."
            )
          )

      val users =
        ingestion
          .readUsers()
          .getOrElse(
            throw new RuntimeException(
              "Impossible de charger users."
            )
          )

      val products =
        ingestion
          .readProducts()
          .getOrElse(
            throw new RuntimeException(
              "Impossible de charger products."
            )
          )

      val merchants =
        ingestion
          .readMerchants()
          .getOrElse(
            throw new RuntimeException(
              "Impossible de charger merchants."
            )
          )

      println()
      println(
        "VOLUMES DES DONNEES CHARGEES"
      )
      println(
        "------------------------------------------------------------"
      )

      println(
        s"Transactions : ${transactions.count()} lignes"
      )

      println(
        s"Users        : ${users.count()} lignes"
      )

      println(
        s"Products     : ${products.count()} lignes"
      )

      println(
        s"Merchants    : ${merchants.count()} lignes"
      )

      println(
        "------------------------------------------------------------"
      )

      (
        transactions,
        users,
        products,
        merchants
      )
    }
  }


  // ============================================================
  // 6. VALIDATION
  // ============================================================

  /**
   * Validation des quatre datasets.
   *
   * IMPORTANT :
   *
   * DataValidation est une classe qui reçoit SparkSession.
   *
   * Chaque méthode retourne :
   *
   * (DataFrame valide, DataFrame rejeté)
   */
  def runValidation(
      transactions: Dataset[Transaction],
      users: Dataset[User],
      products: Dataset[Product],
      merchants: Dataset[Merchant],
      spark: SparkSession
  ): (
      DataFrame,
      DataFrame,
      DataFrame,
      DataFrame,
      DataFrame,
      DataFrame,
      DataFrame,
      DataFrame
  ) = {

    executeStep(
      "VALIDATION"
    ) {

      val validator =
        new DataValidation(
          spark
        )

      // --------------------------------------------------------
      // Transactions
      // --------------------------------------------------------

      val (
        validTransactions,
        rejectedTransactions
      ) =
        validator
          .validateTransactions(
            transactions
          )

      // --------------------------------------------------------
      // Users
      // --------------------------------------------------------

      val (
        validUsers,
        rejectedUsers
      ) =
        validator
          .validateUsers(
            users
          )

      // --------------------------------------------------------
      // Products
      // --------------------------------------------------------

      val (
        validProducts,
        rejectedProducts
      ) =
        validator
          .validateProducts(
            products
          )

      // --------------------------------------------------------
      // Merchants
      // --------------------------------------------------------

      val (
        validMerchants,
        rejectedMerchants
      ) =
        validator
          .validateMerchants(
            merchants
          )

      // --------------------------------------------------------
      // Résultats
      // --------------------------------------------------------

      println()
      println(
        "RESULTATS DE VALIDATION"
      )
      println(
        "------------------------------------------------------------"
      )

      println(
        s"Transactions : ${validTransactions.count()} valides | " +
        s"${rejectedTransactions.count()} rejetees"
      )

      println(
        s"Users        : ${validUsers.count()} valides | " +
        s"${rejectedUsers.count()} rejetes"
      )

      println(
        s"Products     : ${validProducts.count()} valides | " +
        s"${rejectedProducts.count()} rejetes"
      )

      println(
        s"Merchants    : ${validMerchants.count()} valides | " +
        s"${rejectedMerchants.count()} rejetes"
      )

      println(
        "------------------------------------------------------------"
      )

      (
        validTransactions,
        rejectedTransactions,
        validUsers,
        rejectedUsers,
        validProducts,
        rejectedProducts,
        validMerchants,
        rejectedMerchants
      )
    }
  }


  // ============================================================
  // 7. SAUVEGARDE DES DONNEES VALIDEES
  // ============================================================

  /**
   * Sauvegarde les données validées au format Parquet.
   */
  def saveValidatedData(
      validTransactions: DataFrame,
      validUsers: DataFrame,
      validProducts: DataFrame,
      validMerchants: DataFrame,
      config: Config
  ): Unit = {

    executeStep(
      "SAUVEGARDE DES DONNEES VALIDEES"
    ) {

      val outputPath =
        getOutputPath(
          config
        )

      validTransactions
        .write
        .mode("overwrite")
        .parquet(
          outputPath +
            "/validated_transactions"
        )

      validUsers
        .write
        .mode("overwrite")
        .parquet(
          outputPath +
            "/validated_users"
        )

      validProducts
        .write
        .mode("overwrite")
        .parquet(
          outputPath +
            "/validated_products"
        )

      validMerchants
        .write
        .mode("overwrite")
        .parquet(
          outputPath +
            "/validated_merchants"
        )

      println()
      println(
        "[OK] Donnees validees sauvegardees en Parquet."
      )
    }
  }


  // ============================================================
  // 8. CHARGEMENT DES DONNEES VALIDEES
  // ============================================================

  /**
   * Recharge les données validées depuis Parquet.
   *
   * Les DataFrame sont reconvertis en Dataset typés.
   */
  def loadValidatedData(
      spark: SparkSession,
      config: Config
  ): (
      Dataset[Transaction],
      Dataset[User],
      Dataset[Product],
      Dataset[Merchant]
  ) = {

    import spark.implicits._

    val outputPath =
      getOutputPath(
        config
      )

    println()
    println(
      "CHARGEMENT DES DONNEES VALIDEES"
    )
    println(
      "------------------------------------------------------------"
    )

    val transactions =
      spark
        .read
        .parquet(
          outputPath +
            "/validated_transactions"
        )
        .as[Transaction]

    val users =
      spark
        .read
        .parquet(
          outputPath +
            "/validated_users"
        )
        .as[User]

    val products =
      spark
        .read
        .parquet(
          outputPath +
            "/validated_products"
        )
        .as[Product]

    val merchants =
      spark
        .read
        .parquet(
          outputPath +
            "/validated_merchants"
        )
        .as[Merchant]

    println(
      s"[OK] Transactions : ${transactions.count()}"
    )

    println(
      s"[OK] Users        : ${users.count()}"
    )

    println(
      s"[OK] Products     : ${products.count()}"
    )

    println(
      s"[OK] Merchants    : ${merchants.count()}"
    )

    (
      transactions,
      users,
      products,
      merchants
    )
  }


  // ============================================================
  // 9. RAPPORT DE QUALITE
  // ============================================================

  /**
   * Génération du rapport de qualité.
   */
  def runQualityReport(
      transactions: Dataset[Transaction],
      validTransactions: DataFrame,
      rejectedTransactions: DataFrame,
      users: Dataset[User],
      validUsers: DataFrame,
      rejectedUsers: DataFrame,
      products: Dataset[Product],
      validProducts: DataFrame,
      rejectedProducts: DataFrame,
      merchants: Dataset[Merchant],
      validMerchants: DataFrame,
      rejectedMerchants: DataFrame,
      spark: SparkSession
  ): DataFrame = {

    executeStep(
      "RAPPORT DE QUALITE"
    ) {

      val qualityReportBuilder =
        new DataQualityReport(
          spark
        )

      val datasets =
        Seq(

          (
            "transactions",
            transactions.count(),
            validTransactions,
            rejectedTransactions
          ),

          (
            "users",
            users.count(),
            validUsers,
            rejectedUsers
          ),

          (
            "products",
            products.count(),
            validProducts,
            rejectedProducts
          ),

          (
            "merchants",
            merchants.count(),
            validMerchants,
            rejectedMerchants
          )
        )

      val report =
        qualityReportBuilder
          .buildReport(
            datasets
          )

      println()
      println(
        "============================================================"
      )
      println(
        "              RAPPORT DE QUALITE DES DONNEES"
      )
      println(
        "============================================================"
      )

      report.show(
        truncate = false
      )

      report
    }
  }


  // ============================================================
  // 10. TRANSFORMATION CLASSIQUE
  // ============================================================

  /**
   * Transformation sans optimisation.
   */
  def runTransformation(
      transactions: DataFrame,
      users: DataFrame,
      products: DataFrame,
      merchants: DataFrame,
      spark: SparkSession
  ): DataFrame = {

    executeStep(
      "TRANSFORMATION"
    ) {

      val transformation =
        new DataTransformation(
          spark
        )

      val enrichedData =
        transformation
          .enrichTransactionData(
            transactions,
            users,
            products,
            merchants
          )

      println()
      println(
        s"Transactions enrichies : ${enrichedData.count()}"
      )

      println()
      println(
        "APERCU DES DONNEES ENRICHIES"
      )

      enrichedData
        .select(
          "transaction_id",
          "user_id",
          "product_id",
          "merchant_id",
          "amount",
          "timestamp",
          "location",
          "payment_method",
          "transaction_category",
          "customer_segment",
          "product_category",
          "merchant_category",
          "region"
        )
        .show(
          10,
          truncate = false
        )

      enrichedData
    }
  }


  // ============================================================
  // 11. TRANSFORMATION OPTIMISEE
  // ============================================================

  /**
   * Transformation utilisant :
   *
   * - broadcast()
   * - cache()
   * - shuffle partitions
   */
  def runOptimizedTransformation(
      transactions: DataFrame,
      users: DataFrame,
      products: DataFrame,
      merchants: DataFrame,
      spark: SparkSession,
      config: Config
  ): DataFrame = {

    executeStep(
      "TRANSFORMATION OPTIMISEE"
    ) {

      SparkOptimizations
        .configureShufflePartitions(
          spark,
          config
        )

      val enrichedData =
        SparkOptimizations
          .optimizedEnrichTransactionData(
            transactions,
            users,
            products,
            merchants
          )

      val cachedData =
        SparkOptimizations
          .cacheDataFrame(
            enrichedData
          )

      println()
      println(
        s"Transactions enrichies : ${cachedData.count()}"
      )

      cachedData
    }
  }


  // ============================================================
  // 12. SAUVEGARDE DES DONNEES ENRICHIES
  // ============================================================

  def saveEnrichedData(
      enrichedData: DataFrame,
      config: Config
  ): Unit = {

    executeStep(
      "SAUVEGARDE DES DONNEES ENRICHIES"
    ) {

      val outputPath =
        getOutputPath(
          config
        )

      enrichedData
        .write
        .mode("overwrite")
        .parquet(
          outputPath +
            "/enriched_transactions"
        )

      enrichedData
        .write
        .mode("overwrite")
        .option(
          "header",
          "true"
        )
        .csv(
          outputPath +
            "/enriched_transactions_csv"
        )

      println()
      println(
        "[OK] Donnees enrichies sauvegardees en CSV et Parquet."
      )
    }
  }


  // ============================================================
  // 13. CHARGEMENT DES DONNEES ENRICHIES
  // ============================================================

  def loadEnrichedData(
      spark: SparkSession,
      config: Config
  ): DataFrame = {

    val outputPath =
      getOutputPath(
        config
      )

    spark
      .read
      .parquet(
        outputPath +
          "/enriched_transactions"
      )
  }


  // ============================================================
  // 14. ANALYTIQUE
  // ============================================================

  /**
   * Analyses :
   *
   * - KPI globaux
   * - performance des marchands
   * - analyse de cohortes
   *
   * Les résultats sont sauvegardés en CSV et Parquet.
   */
  def runAnalytics(
      enrichedData: DataFrame,
      transactions: DataFrame,
      spark: SparkSession,
      config: Config
  ): Unit = {

    executeStep(
      "ANALYTIQUE"
    ) {

      val analytics =
        new Analytics(
          spark
        )

      // ========================================================
      // KPI GLOBAUX
      // ========================================================

      println()
      println(
        "============================================================"
      )
      println(
        "                    KPI GLOBAUX"
      )
      println(
        "============================================================"
      )

      val globalKpi =
        enrichedData
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
            ),

            round(
              avg("amount"),
              2
            ).alias(
              "average_transaction_amount"
            )
          )
          .select(
            format_number(
              col("total_revenue"),
              2
            ).alias("total_revenue"),
            col("transaction_count"),
            col("unique_customers"),
            col("average_transaction_amount")
          )
      globalKpi.show(
        truncate = false
      )

      // ========================================================
      // PERFORMANCE DES MARCHANDS
      // ========================================================

      println()
      println(
        "============================================================"
      )
      println(
        "              PERFORMANCE DES MARCHANDS"
      )
      println(
        "============================================================"
      )

      val merchantReport =
        analytics
          .merchantPerformanceReportWithoutSuspicion(
            enrichedData
          )
      val merchantReportFormatted =
        merchantReport
          .withColumn("total_revenue", round(col("total_revenue"), 2))
          .withColumn("average_transaction_amount", round(col("average_transaction_amount"), 2))
          .withColumn("total_commission", round(col("total_commission"), 2))
     // 3. On sélectionne uniquement les colonnes utiles pour l'affichage 
      merchantReportFormatted
        .select(
          "merchant_id",
          "merchant_name",
          "merchant_category",
          "region",
          "total_revenue",
          "transaction_count",
          "unique_customers",
          "average_transaction_amount",
          "total_commission",
          "rank_in_category",
        )
        .show(
          20,
          truncate = false
        )

      // ========================================================
      // ANALYSE DE COHORTES
      // ========================================================

      println()
      println(
        "============================================================"
      )
      println(
        "                  ANALYSE DE COHORTES"
      )
      println(
        "============================================================"
      )

      val cohortData =
        analytics
          .cohortAnalysis(
            transactions
          )

      cohortData.show(
        20,
        truncate = false
      )

      // ========================================================
      // SAUVEGARDE
      // ========================================================

      val outputPath =
        getOutputPath(
          config
        )

      globalKpi
        .write
        .mode("overwrite")
        .option(
          "header",
          "true"
        )
        .csv(
          outputPath +
            "/global_kpi"
        )

      globalKpi
        .write
        .mode("overwrite")
        .parquet(
          outputPath +
            "/global_kpi_parquet"
        )

      merchantReport
        .write
        .mode("overwrite")
        .option(
          "header",
          "true"
        )
        .csv(
          outputPath +
            "/merchant_performance"
        )

      merchantReport
        .write
        .mode("overwrite")
        .parquet(
          outputPath +
            "/merchant_performance_parquet"
        )

      cohortData
        .write
        .mode("overwrite")
        .option(
          "header",
          "true"
        )
        .csv(
          outputPath +
            "/cohort_analysis"
        )

      cohortData
        .write
        .mode("overwrite")
        .parquet(
          outputPath +
            "/cohort_analysis_parquet"
        )

      println()
      println(
        "[OK] Resultats analytiques sauvegardes en CSV et Parquet."
      )
    }
  }

  // ============================================================
  // 15. COMPARAISON DES PERFORMANCES - QUESTION 5.3
  // ============================================================

  /**
   * QUESTION 5.3
   *
   * Compare deux exécutions du pipeline :
   *
   * 1. Sans optimisation
   * 2. Avec optimisation
   *
   * Les quatre grandes étapes demandées dans l'épreuve
   * sont mesurées séparément :
   *
   *   - Ingestion
   *   - Transformation
   *   - Analytique
   *   - Écriture
   *
   * Le tableau final présente :
   *
   *   Étape
   *   Durée sans optimisation
   *   Durée avec optimisation
   *   Gain en pourcentage
   *
   * Formule :
   *
   * Gain (%) =
   * ((Temps sans optimisation - Temps avec optimisation)
   *  / Temps sans optimisation) * 100
   */
  def runComparison(
      spark: SparkSession,
      config: Config
  ): Unit = {

    println()

    println(
      "################################################################"
    )

    println(
      "#       COMPARAISON DES PERFORMANCES - QUESTION 5.3           #"
    )

    println(
      "################################################################"
    )


    // ==========================================================
    // STRUCTURE DES MESURES
    // ==========================================================

    case class PerformanceMetrics(
        ingestion: Double,
        transformation: Double,
        analytics: Double,
        writing: Double
    ) {

      def total: Double =
        ingestion +
          transformation +
          analytics +
          writing
    }


    // ==========================================================
    // FONCTION DE CHRONOMETRAGE
    // ==========================================================

    def measureTime[T](
        operation: => T
    ): (T, Double) = {

      val start =
        System.nanoTime()

      val result =
        operation

      val end =
        System.nanoTime()

      (
        result,
        (end - start).toDouble / 1e9
      )
    }


    // ==========================================================
    // CALCUL DU GAIN
    // ==========================================================

    def calculateGain(
        before: Double,
        after: Double
    ): Double = {

      if (before <= 0.0) {

        0.0

      } else {

        (
          (before - after) /
            before
        ) * 100.0
      }
    }


    // ==========================================================
    // EXECUTION D'UNE VERSION
    // ==========================================================

    def executeComparisonRun(
        optimized: Boolean
    ): PerformanceMetrics = {

      // --------------------------------------------------------
      // Nettoyage du cache avant chaque exécution.
      // --------------------------------------------------------

      spark.catalog.clearCache()


      // --------------------------------------------------------
      // Configuration du shuffle uniquement pour la version
      // optimisée.
      // --------------------------------------------------------

      if (optimized) {

        SparkOptimizations
          .configureShufflePartitions(
            spark,
            config
          )
      }


      val mode =
        if (optimized) {

          "AVEC OPTIMISATIONS"

        } else {

          "SANS OPTIMISATION"
        }


      println()

      println(
        "============================================================"
      )

      println(
        s"EXECUTION $mode"
      )

      println(
        "============================================================"
      )


      // ========================================================
      // 1. INGESTION
      // ========================================================

      val (
        ingestionData,
        ingestionTime
      ) =
        measureTime {

          val ingestion =
            new DataIngestion(
              spark
            )


          val transactions =
            ingestion
              .readTransactions()
              .getOrElse(
                throw new RuntimeException(
                  "Impossible de charger transactions."
                )
              )


          val users =
            ingestion
              .readUsers()
              .getOrElse(
                throw new RuntimeException(
                  "Impossible de charger users."
                )
              )


          val products =
            ingestion
              .readProducts()
              .getOrElse(
                throw new RuntimeException(
                  "Impossible de charger products."
                )
              )


          val merchants =
            ingestion
              .readMerchants()
              .getOrElse(
                throw new RuntimeException(
                  "Impossible de charger merchants."
                )
              )


          // ----------------------------------------------------
          // Matérialisation des quatre sources.
          //
          // Cela permet d'obtenir une mesure réelle et
          // comparable de l'ingestion dans les deux modes.
          // ----------------------------------------------------

          val transactionCount =
            transactions.count()

          val userCount =
            users.count()

          val productCount =
            products.count()

          val merchantCount =
            merchants.count()


          println(
            s"[OK] transactions : $transactionCount lignes lues"
          )

          println(
            s"[OK] users : $userCount lignes lues"
          )

          println(
            s"[OK] products : $productCount lignes lues"
          )

          println(
            s"[OK] merchants : $merchantCount lignes lues"
          )


          (
            transactions.toDF(),
            users.toDF(),
            products.toDF(),
            merchants.toDF()
          )
        }


      val transactions =
        ingestionData._1

      val users =
        ingestionData._2

      val products =
        ingestionData._3

      val merchants =
        ingestionData._4


      println()

      println(
        f"Ingestion       : $ingestionTime%.3f s"
      )


      // ========================================================
      // 2. TRANSFORMATION
      // ========================================================

      val (
        enrichedData,
        transformationTime
      ) =
        measureTime {

          if (optimized) {

            println(
              "  -> broadcast() + cache() + shuffle configure."
            )


            // --------------------------------------------------
            // Transformation optimisée.
            // --------------------------------------------------

            val optimizedData =
              SparkOptimizations
                .optimizedEnrichTransactionData(
                  transactions,
                  users,
                  products,
                  merchants
                )


            // --------------------------------------------------
            // CORRECTION IMPORTANTE
            //
            // Analytics utilise age_group.
            //
            // La version optimisée de SparkOptimizations
            // ne créait pas cette colonne.
            //
            // On la recrée ici à partir de age afin que les
            // deux versions produisent une structure compatible.
            // --------------------------------------------------

            val optimizedWithAgeGroup =
              if (
                optimizedData.columns
                  .contains("age_group")
              ) {

                optimizedData

              } else {

                optimizedData
                  .withColumn(
                    "age_group",
                    when(
                      col("age") < 25,
                      "Jeune"
                    )
                      .when(
                        col("age").between(
                          26,
                          44
                        ),
                        "Adulte"
                      )
                      .when(
                        col("age").between(
                          45,
                          64
                        ),
                        "Âge Moyen"
                      )
                      .when(
                        col("age") >= 65,
                        "Senior"
                      )
                      .otherwise(
                        null
                      )
                  )
              }


            // --------------------------------------------------
            // Mise en cache du résultat optimisé.
            // --------------------------------------------------

            val cachedData =
              SparkOptimizations
                .cacheDataFrame(
                  optimizedWithAgeGroup
                )


            // Matérialisation du cache.
            cachedData.count()


            cachedData

          } else {

            println(
              "  -> Jointures classiques, sans cache."
            )


            // --------------------------------------------------
            // Transformation classique.
            // --------------------------------------------------

            val transformation =
              new DataTransformation(
                spark
              )


            val data =
              transformation
                .enrichTransactionData(
                  transactions,
                  users,
                  products,
                  merchants
                )


            // --------------------------------------------------
            // Matérialisation pour obtenir une mesure réelle.
            // --------------------------------------------------

            data.count()


            data
          }
        }


      println()

      println(
        f"Transformation  : $transformationTime%.3f s"
      )


      // ========================================================
      // 3. ANALYTIQUE
      // ========================================================

      val (
        analyticsResult,
        analyticsTime
      ) =
        measureTime {

          val analytics =
            new Analytics(
              spark
            )


          // ----------------------------------------------------
          // KPI globaux
          // ----------------------------------------------------

          val globalKpi =
            enrichedData
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
                ),

                round(
                  avg("amount"),
                  2
                ).alias(
                  "average_transaction_amount"
                )
              )


          // ----------------------------------------------------
          // Performance des marchands
          // ----------------------------------------------------

          val merchantReport =
            analytics
              .merchantPerformanceReportWithoutSuspicion(
                enrichedData
              )


          // ----------------------------------------------------
          // Analyse de cohortes
          // ----------------------------------------------------

          val cohortData =
            analytics
              .cohortAnalysis(
                transactions
              )


          // ----------------------------------------------------
          // Matérialisation des trois résultats.
          // ----------------------------------------------------

          globalKpi.count()

          merchantReport.count()

          cohortData.count()


          (
            globalKpi,
            merchantReport,
            cohortData
          )
        }


      println()

      println(
        f"Analytique      : $analyticsTime%.3f s"
      )


      // ========================================================
      // 4. ECRITURE
      // ========================================================

      val (
        _,
        writingTime
      ) =
        measureTime {

          val outputPath =
            getOutputPath(
              config
            )


          val versionPath =
            if (optimized) {

              outputPath +
                "/comparison_optimized"

            } else {

              outputPath +
                "/comparison_without_optimization"
            }


          val globalKpi =
            analyticsResult._1

          val merchantReport =
            analyticsResult._2

          val cohortData =
            analyticsResult._3


          // ----------------------------------------------------
          // Sauvegarde des KPI globaux.
          // ----------------------------------------------------

          globalKpi
            .write
            .mode("overwrite")
            .option(
              "header",
              "true"
            )
            .csv(
              versionPath +
                "/global_kpi"
            )


          // ----------------------------------------------------
          // Sauvegarde de la performance des marchands.
          // ----------------------------------------------------

          merchantReport
            .write
            .mode("overwrite")
            .option(
              "header",
              "true"
            )
            .csv(
              versionPath +
                "/merchant_performance"
            )


          // ----------------------------------------------------
          // Sauvegarde de l'analyse de cohortes.
          // ----------------------------------------------------

          cohortData
            .write
            .mode("overwrite")
            .option(
              "header",
              "true"
            )
            .csv(
              versionPath +
                "/cohort_analysis"
            )


          println()

          println(
            s"  -> Resultats ecrits dans : $versionPath"
          )


          // Le bloc measureTime doit retourner Unit.
          ()
        }


      println()

      println(
        f"Ecriture        : $writingTime%.3f s"
      )


      // ========================================================
      // TOTAL DE CETTE EXECUTION
      // ========================================================

      val total =
        ingestionTime +
          transformationTime +
          analyticsTime +
          writingTime


      println()

      println(
        f"TOTAL           : $total%.3f s"
      )


      PerformanceMetrics(
        ingestion =
          ingestionTime,

        transformation =
          transformationTime,

        analytics =
          analyticsTime,

        writing =
          writingTime
      )
    }


    // ==========================================================
    // EXECUTION SANS OPTIMISATION
    // ==========================================================

    val withoutOptimization =
      executeComparisonRun(
        optimized = false
      )


    // ==========================================================
    // NETTOYAGE ENTRE LES DEUX EXECUTIONS
    // ==========================================================

    spark.catalog.clearCache()

    System.gc()


    // ==========================================================
    // EXECUTION AVEC OPTIMISATIONS
    // ==========================================================

    val withOptimization =
      executeComparisonRun(
        optimized = true
      )


    // ==========================================================
    // CALCUL DES GAINS
    // ==========================================================

    val ingestionGain =
      calculateGain(
        withoutOptimization.ingestion,
        withOptimization.ingestion
      )


    val transformationGain =
      calculateGain(
        withoutOptimization.transformation,
        withOptimization.transformation
      )


    val analyticsGain =
      calculateGain(
        withoutOptimization.analytics,
        withOptimization.analytics
      )


    val writingGain =
      calculateGain(
        withoutOptimization.writing,
        withOptimization.writing
      )


    val totalGain =
      calculateGain(
        withoutOptimization.total,
        withOptimization.total
      )


    // ==========================================================
    // TABLEAU FINAL DEMANDE PAR L'EPREUVE
    // ==========================================================

    println()

    println(
      "################################################################"
    )

    println(
      "#       TABLEAU COMPARATIF - QUESTION 5.3                    #"
    )

    println(
      "################################################################"
    )

    println()

    println(
      "+------------------+----------------------+----------------------+--------------+"
    )

    println(
      "| Etape            | Sans optimisation (s)| Avec optimisation (s)| Gain (%)     |"
    )

    println(
      "+------------------+----------------------+----------------------+--------------+"
    )


    println(
      f"| Ingestion        | ${withoutOptimization.ingestion}%20.3f | ${withOptimization.ingestion}%20.3f | ${ingestionGain}%10.2f |"
    )

    println(
      "+------------------+----------------------+----------------------+--------------+"
    )


    println(
      f"| Transformation   | ${withoutOptimization.transformation}%20.3f | ${withOptimization.transformation}%20.3f | ${transformationGain}%10.2f |"
    )

    println(
      "+------------------+----------------------+----------------------+--------------+"
    )


    println(
      f"| Analytique       | ${withoutOptimization.analytics}%20.3f | ${withOptimization.analytics}%20.3f | ${analyticsGain}%10.2f |"
    )

    println(
      "+------------------+----------------------+----------------------+--------------+"
    )


    println(
      f"| Ecriture         | ${withoutOptimization.writing}%20.3f | ${withOptimization.writing}%20.3f | ${writingGain}%10.2f |"
    )

    println(
      "+------------------+----------------------+----------------------+--------------+"
    )


    println(
      f"| TOTAL            | ${withoutOptimization.total}%20.3f | ${withOptimization.total}%20.3f | ${totalGain}%10.2f |"
    )

    println(
      "+------------------+----------------------+----------------------+--------------+"
    )


    // ==========================================================
    // EXPLICATION DU RESULTAT
    // ==========================================================

    println()

    println(
      "Formule du gain :"
    )

    println(
      "Gain (%) = ((Temps sans optimisation - Temps avec optimisation)"
    )

    println(
      "            / Temps sans optimisation) * 100"
    )


    println()

    println(
      "Optimisations utilisées dans la seconde execution :"
    )

    println(
      "  - broadcast() des petites tables"
    )

    println(
      "  - cache() du DataFrame enrichi"
    )

    println(
      "  - configuration de spark.sql.shuffle.partitions"
    )


    println()

    println(
      "================================================================"
    )

    println(
      "#             FIN DE LA COMPARAISON QUESTION 5.3             #"
    )

    println(
      "================================================================"
    )


    // ----------------------------------------------------------
    // Nettoyage final du cache.
    // ----------------------------------------------------------

    spark.catalog.clearCache()
  }


  // ============================================================
  // 16. PIPELINE COMPLET - QUESTION 6.1
  // ============================================================

  /**
   * Orchestration complète :
   *
   * 1. Ingestion
   * 2. Validation
   * 3. Sauvegarde des données validées
   * 4. Rapport qualité
   * 5. Transformation
   * 6. Sauvegarde des données enrichies
   * 7. Analytique
   */
  def runAll(
      spark: SparkSession,
      config: Config
  ): Unit = {

    println()
    println(
      "################################################################"
    )
    println(
      "#                  PIPELINE COMPLET                            #"
    )
    println(
      "################################################################"
    )


    // ==========================================================
    // ETAPE 1 : INGESTION
    // ==========================================================

    val (
      transactions,
      users,
      products,
      merchants
    ) =
      runIngestion(
        spark
      )


    // ==========================================================
    // ETAPE 2 : VALIDATION
    // ==========================================================

    val (
      validTransactions,
      rejectedTransactions,
      validUsers,
      rejectedUsers,
      validProducts,
      rejectedProducts,
      validMerchants,
      rejectedMerchants
    ) =
      runValidation(
        transactions,
        users,
        products,
        merchants,
        spark
      )


    // ==========================================================
    // ETAPE 3 : SAUVEGARDE DES DONNEES VALIDEES
    // ==========================================================

    saveValidatedData(
      validTransactions,
      validUsers,
      validProducts,
      validMerchants,
      config
    )


    // ==========================================================
    // ETAPE 4 : RAPPORT QUALITE
    // ==========================================================

    runQualityReport(
      transactions,
      validTransactions,
      rejectedTransactions,
      users,
      validUsers,
      rejectedUsers,
      products,
      validProducts,
      rejectedProducts,
      merchants,
      validMerchants,
      rejectedMerchants,
      spark
    )


    // ==========================================================
    // ETAPE 5 : TRANSFORMATION
    // ==========================================================

    val enrichedData =
      runTransformation(
        validTransactions,
        validUsers,
        validProducts,
        validMerchants,
        spark
      )


    // ==========================================================
    // ETAPE 6 : SAUVEGARDE DES DONNEES ENRICHIES
    // ==========================================================

    saveEnrichedData(
      enrichedData,
      config
    )


    // ==========================================================
    // ETAPE 7 : ANALYTIQUE
    // ==========================================================

    runAnalytics(
      enrichedData,
      validTransactions,
      spark,
      config
    )


    println()
    println(
      "################################################################"
    )
    println(
      "#              PIPELINE COMPLET TERMINE                       #"
    )
    println(
      "################################################################"
    )
  }


  // ============================================================
  // 17. EXECUTION MODULAIRE - QUESTION 6.2
  // ============================================================

  def runMode(
      mode: String,
      spark: SparkSession,
      config: Config
  ): Unit = {

    mode match {

      // ========================================================
      // INGESTION
      // ========================================================

      case "ingestion" =>

        runIngestion(
          spark
        )


      // ========================================================
      // TRANSFORMATION
      // ========================================================

      case "transformation" =>

        println()
        println(
          "============================================================"
        )
        println(
          "MODE TRANSFORMATION"
        )
        println(
          "============================================================"
        )

        val (
          transactions,
          users,
          products,
          merchants
        ) =
          loadValidatedData(
            spark,
            config
          )

        val enrichedData =
          runTransformation(
            transactions.toDF(),
            users.toDF(),
            products.toDF(),
            merchants.toDF(),
            spark
          )

        saveEnrichedData(
          enrichedData,
          config
        )


      // ========================================================
      // ANALYTICS
      // ========================================================

      case "analytics" =>

        println()
        println(
          "============================================================"
        )
        println(
          "MODE ANALYTICS"
        )
        println(
          "============================================================"
        )

        val enrichedData =
          loadEnrichedData(
            spark,
            config
          )

        val (
          transactions,
          _,
          _,
          _
        ) =
          loadValidatedData(
            spark,
            config
          )

        runAnalytics(
          enrichedData,
          transactions.toDF(),
          spark,
          config
        )


      // ========================================================
      // COMPARAISON
      // ========================================================

      case "comparaison" =>

        runComparison(
          spark,
          config
        )


      // ========================================================
      // ALL
      // ========================================================

      case "all" =>

        runAll(
          spark,
          config
        )


      // ========================================================
      // MODE INCONNU
      // ========================================================

      case _ =>

        println()
        println(
          s"[ERREUR] Mode d'execution inconnu : $mode"
        )

        printHelp()
    }
  }


  // ============================================================
  // 18. MAIN
  // ============================================================

  def main(
      args: Array[String]
  ): Unit = {

    val config =
      ConfigFactory.load()

    val mode =
      if (
        args.isEmpty
      ) {

        "all"

      } else {

        args(0)
          .trim
          .toLowerCase
      }


    val acceptedModes =
      Set(
        "ingestion",
        "transformation",
        "analytics",
        "comparaison",
        "all"
      )


    if (
      !acceptedModes.contains(
        mode
      )
    ) {

      println()
      println(
        s"[ERREUR] Argument inconnu : $mode"
      )

      printHelp()

      return
    }


    val spark =
      createSparkSession(
        config
      )

    spark.sparkContext.setLogLevel(
      "WARN"
    )


    try {

      println()
      println(
        "============================================================"
      )
      println(
        "       ECOMMERCE ANALYTICS - APPLICATION PRINCIPALE"
      )
      println(
        "============================================================"
      )

      println(
        s"Mode d'execution : $mode"
      )

      println(
        s"Application Spark : " +
        s"${spark.sparkContext.appName}"
      )

      println(
        s"Shuffle partitions : " +
        s"${spark.conf.get(
          "spark.sql.shuffle.partitions"
        )}"
      )


      runMode(
        mode,
        spark,
        config
      )


      println()
      println(
        "============================================================"
      )
      println(
        "              EXECUTION TERMINEE AVEC SUCCES"
      )
      println(
        "============================================================"
      )


    } catch {

      case e: Exception =>

        println()
        println(
          "============================================================"
        )
        println(
          "                    ERREUR GLOBALE"
        )
        println(
          "============================================================"
        )

        println(
          s"Type : ${e.getClass.getSimpleName}"
        )

        println(
          s"Message : ${e.getMessage}"
        )

        println()

        println(
          "Le pipeline a ete interrompu."
        )

        e.printStackTrace()


    } finally {

      println()

      println(
        "Arret de Spark..."
      )

      spark.stop()

      println(
        "SparkSession arretee proprement."
      )
    }
  }
}
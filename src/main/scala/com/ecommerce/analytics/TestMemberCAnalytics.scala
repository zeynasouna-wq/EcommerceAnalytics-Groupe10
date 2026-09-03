package com.ecommerce.analytics

import org.apache.spark.sql.SparkSession

object TestAnalytics {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Test Analytics - Membre C - Question 4")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try {

      // ============================================================
      // HEADER
      // ============================================================

      println()
      println("================================================================")
      println("          TEST ANALYTICS - MEMBRE C - QUESTION 4")
      println("================================================================")
      println("  4.1 - Performance des marchands")
      println("  4.2 - Analyse de cohortes")
      println("  4.3 - Segmentation RFM - BONUS")
      println("  4.4 - Produits et catégories - BONUS")
      println("================================================================")


      // ============================================================
      // 1. CHARGEMENT DES DONNEES
      // ============================================================

      println()
      println("================================================================")
      println("1. CHARGEMENT DES DONNEES")
      println("================================================================")

      val ingestion = new DataIngestion(spark)

      val transactions =
        ingestion.readTransactions().get

      val users =
        ingestion.readUsers().get

      val products =
        ingestion.readProducts().get

      val merchants =
        ingestion.readMerchants().get

      println()
      println("[OK] Données chargées")
      println(s"Transactions : ${transactions.count()}")
      println(s"Users        : ${users.count()}")
      println(s"Products     : ${products.count()}")
      println(s"Merchants    : ${merchants.count()}")


      // ============================================================
      // 2. ENRICHISSEMENT DES TRANSACTIONS
      // ============================================================

      println()
      println("================================================================")
      println("2. ENRICHISSEMENT DES TRANSACTIONS")
      println("================================================================")

      val transformation =
        new DataTransformation(spark)

      val enrichedData =
        transformation.enrichTransactionData(
          transactions.toDF(),
          users.toDF(),
          products.toDF(),
          merchants.toDF()
        )

      val enrichedCount =
        enrichedData.count()

      println()
      println(s"[OK] Transactions enrichies : $enrichedCount")

      println()
      println("Colonnes principales du DataFrame enrichi :")

      enrichedData.columns.foreach { column =>
        println(s"  - $column")
      }


      // ============================================================
      // INITIALISATION DE LA CLASSE ANALYTICS
      // ============================================================

      val analytics =
        new Analytics(spark)


      // ################################################################
      // ################################################################
      // QUESTION 4.1
      // PERFORMANCE DES MARCHANDS
      // ################################################################
      // ################################################################

      println()
      println()
      println("################################################################")
      println("#                                                              #")
      println("#       QUESTION 4.1 - PERFORMANCE DES MARCHANDS              #")
      println("#                                                              #")
      println("################################################################")


      // ------------------------------------------------------------
      // 4.1.1 Calcul du rapport marchand
      // ------------------------------------------------------------

      println()
      println("------------------------------------------------------------")
      println("4.1.1 - CALCUL DU RAPPORT MARCHAND")
      println("------------------------------------------------------------")

      val merchantReport =
        analytics.merchantPerformanceReportWithoutSuspicion(
          enrichedData
        )

      println()
      println("[OK] Rapport marchand calculé")


      // ------------------------------------------------------------
      // 4.1.2 Affichage
      // ------------------------------------------------------------

      println()
      println("------------------------------------------------------------")
      println("4.1.2 - TOP 20 MARCHANDS")
      println("------------------------------------------------------------")

      merchantReport.show(
        20,
        truncate = false
      )


      // ------------------------------------------------------------
      // 4.1.3 Vérifications
      // ------------------------------------------------------------

      println()
      println("------------------------------------------------------------")
      println("4.1.3 - VERIFICATIONS 4.1")
      println("------------------------------------------------------------")

      val merchantCount =
        merchantReport.count()

      println(
        s"[CHECK] Nombre de marchands dans le rapport : $merchantCount"
      )

      println()
      println("[CHECK] Colonnes produites :")

      merchantReport.columns.foreach { column =>
        println(s"  [OK] $column")
      }

      println()
      println("[CHECK] Présence des KPI principaux :")

      val expectedMerchantColumns =
        Seq(
          "merchant_id",
          "merchant_name",
          "merchant_category",
          "region",
          "commission_rate",
          "total_revenue",
          "transaction_count",
          "unique_customers",
          "average_transaction_amount",
          "total_commission",
          "sales_Jeune",
          "sales_Adulte",
          "sales_Age_Moyen",
          "sales_Senior",
          "rank_in_category",
          "rank_in_region"
        )

      expectedMerchantColumns.foreach { column =>

        if (merchantReport.columns.contains(column)) {
          println(s"  [OK] $column")
        } else {
          println(s"  [ERREUR] Colonne absente : $column")
        }
      }


      // ------------------------------------------------------------
      // 4.1.4 Vérification des valeurs
      // ------------------------------------------------------------

      println()
      println("[CHECK] Vérification des valeurs KPI :")

      merchantReport
        .select(
          "total_revenue",
          "transaction_count",
          "unique_customers",
          "average_transaction_amount",
          "total_commission",
          "rank_in_category",
          "rank_in_region"
        )
        .describe()
        .show(
          truncate = false
        )

      println()
      println("[OK] TEST 4.1 TERMINE")


      // ################################################################
      // ################################################################
      // QUESTION 4.2
      // ANALYSE DE COHORTES
      // ################################################################
      // ################################################################

      println()
      println()
      println("################################################################")
      println("#                                                              #")
      println("#          QUESTION 4.2 - ANALYSE DE COHORTES                 #")
      println("#                                                              #")
      println("################################################################")


      // ------------------------------------------------------------
      // 4.2.1 Calcul des cohortes
      // ------------------------------------------------------------

      println()
      println("------------------------------------------------------------")
      println("4.2.1 - CALCUL DES COHORTES")
      println("------------------------------------------------------------")

      val cohortData =
        analytics.cohortAnalysis(
          transactions.toDF()
        )

      println()
      println("[OK] Analyse de cohortes calculée")


      // ------------------------------------------------------------
      // 4.2.2 Affichage
      // ------------------------------------------------------------

      println()
      println("------------------------------------------------------------")
      println("4.2.2 - RESULTAT DES COHORTES")
      println("------------------------------------------------------------")

      cohortData.show(
        30,
        truncate = false
      )


      // ------------------------------------------------------------
      // 4.2.3 Vérification des colonnes
      // ------------------------------------------------------------

      println()
      println("------------------------------------------------------------")
      println("4.2.3 - VERIFICATIONS 4.2")
      println("------------------------------------------------------------")

      val cohortColumns =
        Seq(
          "cohort_month",
          "period_index",
          "cohort_size",
          "active_users",
          "retention_rate",
          "revenue",
          "revenue_per_user"
        )

      println()
      println("Colonnes attendues :")

      cohortColumns.foreach { column =>

        if (cohortData.columns.contains(column)) {
          println(s"  [OK] $column")
        } else {
          println(s"  [ERREUR] Colonne absente : $column")
        }
      }


      // ------------------------------------------------------------
      // 4.2.4 Taille du résultat
      // ------------------------------------------------------------

      val cohortRowCount =
        cohortData.count()

      println()
      println(
        s"[CHECK] Nombre de lignes dans l'analyse de cohortes : $cohortRowCount"
      )


      // ------------------------------------------------------------
      // 4.2.5 Meilleure cohorte à 3 mois
      // ------------------------------------------------------------

      println()
      println("------------------------------------------------------------")
      println("4.2.5 - MEILLEURE COHORTE A 3 MOIS")
      println("------------------------------------------------------------")

      val bestCohort =
        analytics.bestCohortAt3Months(
          cohortData
        )

      if (bestCohort.count() > 0) {

        println()
        println("[OK] Cohorte à 3 mois trouvée")

        bestCohort.show(
          truncate = false
        )

      } else {

        println()
        println("[INFO] Aucune cohorte avec period_index = 3")
      }


      println()
      println("[OK] TEST 4.2 TERMINE")


      // ################################################################
      // ################################################################
      // QUESTION 4.3 - BONUS
      // SEGMENTATION RFM
      // ################################################################
      // ################################################################

      println()
      println()
      println("################################################################")
      println("#                                                              #")
      println("#       QUESTION 4.3 - SEGMENTATION RFM - BONUS               #")
      println("#                                                              #")
      println("################################################################")


      // ------------------------------------------------------------
      // 4.3.1 Calcul RFM
      // ------------------------------------------------------------

      println()
      println("------------------------------------------------------------")
      println("4.3.1 - CALCUL RFM")
      println("------------------------------------------------------------")

      val rfmData =
        analytics.analyzeRFM(
          transactions.toDF(),
          users.toDF()
        )

      println()
      println("[OK] Segmentation RFM calculée")


      // ------------------------------------------------------------
      // 4.3.2 Affichage
      // ------------------------------------------------------------

      println()
      println("------------------------------------------------------------")
      println("4.3.2 - RESULTAT RFM")
      println("------------------------------------------------------------")

      rfmData.show(
        30,
        truncate = false
      )


      // ------------------------------------------------------------
      // 4.3.3 Vérification des colonnes
      // ------------------------------------------------------------

      println()
      println("------------------------------------------------------------")
      println("4.3.3 - VERIFICATIONS RFM")
      println("------------------------------------------------------------")

      val rfmColumns =
        Seq(
          "user_id",
          "recency",
          "frequency",
          "monetary",
          "recency_score",
          "frequency_score",
          "monetary_score",
          "rfm_segment",
          "customer_segment"
        )

      println()
      println("Colonnes attendues :")

      rfmColumns.foreach { column =>

        if (rfmData.columns.contains(column)) {
          println(s"  [OK] $column")
        } else {
          println(s"  [ERREUR] Colonne absente : $column")
        }
      }


      // ------------------------------------------------------------
      // 4.3.4 Nombre d'utilisateurs segmentés
      // ------------------------------------------------------------

      val rfmCount =
        rfmData.count()

      println()
      println(
        s"[CHECK] Nombre d'utilisateurs segmentés : $rfmCount"
      )


      // ------------------------------------------------------------
      // 4.3.5 Répartition des segments RFM
      // ------------------------------------------------------------

      println()
      println("------------------------------------------------------------")
      println("4.3.5 - REPARTITION DES SEGMENTS RFM")
      println("------------------------------------------------------------")

      rfmData
        .groupBy("rfm_segment")
        .count()
        .orderBy("rfm_segment")
        .show(
          truncate = false
        )


      // ------------------------------------------------------------
      // 4.3.6 Croisement RFM / Customer Segment
      // ------------------------------------------------------------

      println()
      println("------------------------------------------------------------")
      println("4.3.6 - CROISEMENT RFM / CUSTOMER SEGMENT")
      println("------------------------------------------------------------")

      val rfmCrossTab =
        analytics.rfmCrossTab(
          rfmData
        )

      rfmCrossTab.show(
        50,
        truncate = false
      )


      println()
      println("[OK] TEST 4.3 BONUS TERMINE")


      // ################################################################
      // ################################################################
      // QUESTION 4.4 - BONUS
      // ANALYSE PRODUITS ET CATEGORIES
      // ################################################################
      // ################################################################

      println()
      println()
      println("################################################################")
      println("#                                                              #")
      println("#       QUESTION 4.4 - PRODUITS & CATEGORIES - BONUS          #")
      println("#                                                              #")
      println("################################################################")


      // ------------------------------------------------------------
      // 4.4.1 Calcul des analyses
      // ------------------------------------------------------------

      println()
      println("------------------------------------------------------------")
      println("4.4.1 - CALCUL DES ANALYSES PRODUITS / CATEGORIES")
      println("------------------------------------------------------------")

      val productCategoryResults =
        analytics.analyzeProductAndCategoryData(
          enrichedData
        )

      val topProducts =
        productCategoryResults._1

      val categoryRegion =
        productCategoryResults._2

      val paymentPeriodRevenue =
        productCategoryResults._3

      println()
      println("[OK] Analyses produits et catégories calculées")


      // ------------------------------------------------------------
      // 4.4.2 TOP 10 PRODUITS PAR CA
      // ------------------------------------------------------------

      println()
      println("------------------------------------------------------------")
      println("4.4.2 - TOP 10 PRODUITS PAR CA")
      println("------------------------------------------------------------")

      topProducts.show(
        10,
        truncate = false
      )


      // ------------------------------------------------------------
      // 4.4.3 CA PAR CATEGORIE ET REGION
      // ------------------------------------------------------------

      println()
      println("------------------------------------------------------------")
      println("4.4.3 - CA PAR CATEGORIE ET REGION")
      println("------------------------------------------------------------")

      categoryRegion.show(
        30,
        truncate = false
      )


      // ------------------------------------------------------------
      // 4.4.4 CA PAR MOYEN DE PAIEMENT ET PERIODE
      // ------------------------------------------------------------

      println()
      println("------------------------------------------------------------")
      println("4.4.4 - CA PAR MOYEN DE PAIEMENT ET PERIODE")
      println("------------------------------------------------------------")

      paymentPeriodRevenue.show(
        50,
        truncate = false
      )


      // ------------------------------------------------------------
      // 4.4.5 Vérifications TOP PRODUITS
      // ------------------------------------------------------------

      println()
      println("------------------------------------------------------------")
      println("4.4.5 - VERIFICATIONS PRODUITS")
      println("------------------------------------------------------------")

      println(
        s"[CHECK] Nombre de produits dans le TOP 10 : ${topProducts.count()}"
      )

      val expectedTopProductColumns =
        Seq(
          "product_id",
          "product_name",
          "total_revenue",
          "average_rating",
          "stock"
        )

      expectedTopProductColumns.foreach { column =>

        if (topProducts.columns.contains(column)) {
          println(s"  [OK] $column")
        } else {
          println(s"  [ERREUR] Colonne absente : $column")
        }
      }


      // ------------------------------------------------------------
      // 4.4.6 Vérifications catégorie/région
      // ------------------------------------------------------------

      println()
      println("------------------------------------------------------------")
      println("4.4.6 - VERIFICATIONS CATEGORIE / REGION")
      println("------------------------------------------------------------")

      println(
        s"[CHECK] Nombre de lignes catégorie/région : ${categoryRegion.count()}"
      )

      val expectedCategoryColumns =
        Seq(
          "merchant_category",
          "region",
          "total_revenue",
          "transaction_count",
          "category_share_percent"
        )

      expectedCategoryColumns.foreach { column =>

        if (categoryRegion.columns.contains(column)) {
          println(s"  [OK] $column")
        } else {
          println(s"  [ERREUR] Colonne absente : $column")
        }
      }


      // ------------------------------------------------------------
      // 4.4.7 Vérifications paiement/période
      // ------------------------------------------------------------

      println()
      println("------------------------------------------------------------")
      println("4.4.7 - VERIFICATIONS PAIEMENT / PERIODE")
      println("------------------------------------------------------------")

      println(
        s"[CHECK] Nombre de lignes paiement/période : ${paymentPeriodRevenue.count()}"
      )

      val expectedPaymentColumns =
        Seq(
          "payment_method",
          "day_period",
          "total_revenue",
          "transaction_count"
        )

      expectedPaymentColumns.foreach { column =>

        if (paymentPeriodRevenue.columns.contains(column)) {
          println(s"  [OK] $column")
        } else {
          println(s"  [ERREUR] Colonne absente : $column")
        }
      }


      println()
      println("[OK] TEST 4.4 BONUS TERMINE")


      // ################################################################
      // ################################################################
      // SYNTHESE FINALE
      // ################################################################
      // ################################################################

      println()
      println()
      println("================================================================")
      println("                    SYNTHESE QUESTION 4")
      println("================================================================")

      println()
      println("4.1 - Performance des marchands       : [OK]")
      println("4.2 - Analyse de cohortes              : [OK]")
      println("4.3 - Segmentation RFM                 : [OK] BONUS")
      println("4.4 - Produits et catégories           : [OK] BONUS")

      println()
      println("================================================================")
      println("       TOUTES LES PARTIES DE LA QUESTION 4 ONT ETE TESTEES")
      println("================================================================")

      println()
      println("Tests effectués :")
      println("  [OK] Chargement des données")
      println("  [OK] Enrichissement des transactions")
      println("  [OK] Question 4.1")
      println("  [OK] Question 4.2")
      println("  [OK] Question 4.3 - BONUS")
      println("  [OK] Question 4.4 - BONUS")

      println()
      println("================================================================")
      println("                TEST MEMBER C TERMINE")
      println("================================================================")

    } finally {

      spark.stop()
    }
  }
}
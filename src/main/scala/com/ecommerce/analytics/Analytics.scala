package com.ecommerce.analytics

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * ============================================================
 * ANALYTICS & PERFORMANCE ENGINEER - MEMBRE C
 * ============================================================
 *
 * Partie 4 :
 *   - Question 4.1 : KPI marchands
 *   - Question 4.2 : Analyse de cohortes
 *   - Question 4.3 : Segmentation RFM (BONUS)
 *   - Question 4.4 : Analyse produits et catégories (BONUS)
 *
 * Les données utilisées proviennent principalement du DataFrame
 * enrichi produit par DataTransformation.scala.
 */
class Analytics(spark: SparkSession) {

  // ============================================================
  // QUESTION 4.1 - RAPPORT DÉTAILLÉ PAR MARCHAND
  // ============================================================

  /**
   * Calcule les principaux KPI pour chaque marchand.
   *
   * KPI produits :
   *   - chiffre d'affaires total
   *   - nombre de transactions
   *   - nombre de clients uniques
   *   - montant moyen des transactions
   *   - classement dans la catégorie
   *   - classement dans la région
   *   - commission totale
   *   - répartition des ventes par âge
   *
   * Le taux de transactions suspectes est ajouté lorsqu'un
   * DataFrame contenant la colonne is_suspicious est fourni.
   */
  def merchantPerformanceReport(
      enrichedData: DataFrame,
      suspiciousData: DataFrame
  ): DataFrame = {

    // ------------------------------------------------------------
    // 1. KPI DE BASE PAR MARCHAND
    // ------------------------------------------------------------

    val merchantKpis =
      enrichedData
        .filter(col("merchant_id").isNotNull)
        .groupBy(
          "merchant_id",
          "merchant_name",
          "merchant_category",
          "region",
          "commission_rate"
        )
        .agg(

          // Chiffre d'affaires total
          round(sum("amount"), 2)
            .alias("total_revenue"),

          // Nombre total de transactions
          count("*")
            .alias("transaction_count"),

          // Nombre de clients uniques
          countDistinct("user_id")
            .alias("unique_customers"),

          // Montant moyen
          round(avg("amount"), 2)
            .alias("average_transaction_amount"),

          // Commission totale
          round(
            sum(
              col("amount") * col("commission_rate")
            ),
            2
          ).alias("total_commission"),

          // ------------------------------------------------------
          // Répartition des ventes par tranche d'âge
          // ------------------------------------------------------

          sum(
            when(
              col("age_group") === "Jeune",
              col("amount")
            ).otherwise(0)
          ).alias("sales_Jeune"),

          sum(
            when(
              col("age_group") === "Adulte",
              col("amount")
            ).otherwise(0)
          ).alias("sales_Adulte"),

          sum(
            when(
              col("age_group") === "Âge Moyen",
              col("amount")
            ).otherwise(0)
          ).alias("sales_Age_Moyen"),

          sum(
            when(
              col("age_group") === "Senior",
              col("amount")
            ).otherwise(0)
          ).alias("sales_Senior")
        )

    // ------------------------------------------------------------
    // 2. CLASSEMENT PAR CATÉGORIE
    // ------------------------------------------------------------

    val categoryWindow =
      Window
        .partitionBy("merchant_category")
        .orderBy(col("total_revenue").desc)

    // ------------------------------------------------------------
    // 3. CLASSEMENT PAR RÉGION
    // ------------------------------------------------------------

    val regionWindow =
      Window
        .partitionBy("region")
        .orderBy(col("total_revenue").desc)

    val rankedKpis =
      merchantKpis
        .withColumn(
          "rank_in_category",
          dense_rank().over(categoryWindow)
        )
        .withColumn(
          "rank_in_region",
          dense_rank().over(regionWindow)
        )

    // ------------------------------------------------------------
    // 4. TAUX DE TRANSACTIONS SUSPECTES
    // ------------------------------------------------------------

    val suspiciousRates =
      suspiciousData
        .filter(col("merchant_id").isNotNull)
        .groupBy("merchant_id")
        .agg(

          count("*")
            .alias("total_transactions"),

          sum(
            when(
              col("is_suspicious") === 1,
              1
            ).otherwise(0)
          ).alias("suspicious_transactions")
        )
        .withColumn(
          "suspicious_transaction_rate",
          round(
            col("suspicious_transactions") /
              col("total_transactions") * 100,
            2
          )
        )
        .select(
          "merchant_id",
          "suspicious_transaction_rate"
        )

    // ------------------------------------------------------------
    // 5. RAPPORT FINAL
    // ------------------------------------------------------------

    rankedKpis
      .join(
        suspiciousRates,
        Seq("merchant_id"),
        "left"
      )
      .na.fill(
        0.0,
        Seq("suspicious_transaction_rate")
      )
      .orderBy(
        col("total_revenue").desc
      )
  }


  /**
   * Version simplifiée de 4.1 lorsque l'on ne dispose pas
   * encore du DataFrame des transactions suspectes.
   *
   * Utile pour tester la question 4.1 avant d'avoir terminé
   * toute l'intégration du bonus 3.4.
   */
  def merchantPerformanceReportWithoutSuspicion(
      enrichedData: DataFrame
  ): DataFrame = {

    val merchantKpis =
      enrichedData
        .filter(col("merchant_id").isNotNull)
        .groupBy(
          "merchant_id",
          "merchant_name",
          "merchant_category",
          "region",
          "commission_rate"
        )
        .agg(

          round(sum("amount"), 2)
            .alias("total_revenue"),

          count("*")
            .alias("transaction_count"),

          countDistinct("user_id")
            .alias("unique_customers"),

          round(avg("amount"), 2)
            .alias("average_transaction_amount"),

          round(
            sum(
              col("amount") * col("commission_rate")
            ),
            2
          ).alias("total_commission"),

          sum(
            when(col("age_group") === "Jeune", col("amount"))
              .otherwise(0)
          ).alias("sales_Jeune"),

          sum(
            when(col("age_group") === "Adulte", col("amount"))
              .otherwise(0)
          ).alias("sales_Adulte"),

          sum(
            when(col("age_group") === "Âge Moyen", col("amount"))
              .otherwise(0)
          ).alias("sales_Age_Moyen"),

          sum(
            when(col("age_group") === "Senior", col("amount"))
              .otherwise(0)
          ).alias("sales_Senior")
        )

    val categoryWindow =
      Window
        .partitionBy("merchant_category")
        .orderBy(col("total_revenue").desc)

    val regionWindow =
      Window
        .partitionBy("region")
        .orderBy(col("total_revenue").desc)

    merchantKpis
      .withColumn(
        "rank_in_category",
        dense_rank().over(categoryWindow)
      )
      .withColumn(
        "rank_in_region",
        dense_rank().over(regionWindow)
      )
      .orderBy(
        col("total_revenue").desc
      )
  }


  // ============================================================
  // QUESTION 4.2 - ANALYSE DE COHORTES
  // ============================================================

  /**
   * Analyse de cohortes utilisateurs.
   *
   * Pour chaque utilisateur :
   *   1. Identification du mois de première transaction.
   *   2. Calcul du period_index.
   *   3. Calcul du nombre d'utilisateurs actifs.
   *   4. Calcul du taux de rétention.
   *
   * Le résultat contient également :
   *   - le CA de la cohorte/période
   *   - le revenu moyen par utilisateur
   *
   * Ces deux dernières colonnes correspondent au bonus de 4.2.
   */
  def cohortAnalysis(
      transactions: DataFrame
  ): DataFrame = {

    // ------------------------------------------------------------
    // 1. CONVERSION DU TIMESTAMP
    // ------------------------------------------------------------

    val prepared =
      transactions
        .withColumn(
          "transaction_datetime",
          to_timestamp(
            substring(
              col("timestamp"),
              1,
              14
            ),
            "yyyyMMddHHmmss"
          )
        )
        .filter(
          col("transaction_datetime").isNotNull
        )
        .withColumn(
          "transaction_month",
          trunc(
            col("transaction_datetime"),
            "month"
          )
        )

    // ------------------------------------------------------------
    // 2. IDENTIFICATION DU MOIS DE PREMIÈRE TRANSACTION
    // ------------------------------------------------------------

    val userCohorts =
      prepared
        .groupBy("user_id")
        .agg(
          min("transaction_month")
            .alias("cohort_month")
        )

    // ------------------------------------------------------------
    // 3. ASSOCIATION TRANSACTION / COHORTE
    // ------------------------------------------------------------

    val transactionsWithCohort =
      prepared
        .join(
          userCohorts,
          Seq("user_id"),
          "inner"
        )

    // ------------------------------------------------------------
    // 4. CALCUL DE L'INDICE DE PÉRIODE
    // ------------------------------------------------------------

    val withPeriodIndex =
      transactionsWithCohort
        .withColumn(
          "period_index",
          months_between(
            col("transaction_month"),
            col("cohort_month")
          ).cast("int")
        )

    // ------------------------------------------------------------
    // 5. TAILLE INITIALE DES COHORTES
    // ------------------------------------------------------------

    val cohortSizes =
      userCohorts
        .groupBy("cohort_month")
        .agg(
          countDistinct("user_id")
            .alias("cohort_size")
        )

    // ------------------------------------------------------------
    // 6. ACTIVITÉ PAR COHORTE ET PAR PÉRIODE
    // ------------------------------------------------------------

    val cohortActivity =
      withPeriodIndex
        .groupBy(
          "cohort_month",
          "period_index"
        )
        .agg(

          // Un utilisateur est compté une seule fois
          // par période.
          countDistinct("user_id")
            .alias("active_users"),

          // CA généré
          round(sum("amount"), 2)
            .alias("revenue")
        )

    // ------------------------------------------------------------
    // 7. CALCUL DE LA RÉTENTION
    // ------------------------------------------------------------

    cohortActivity
      .join(
        cohortSizes,
        Seq("cohort_month"),
        "left"
      )
      .withColumn(
        "retention_rate",
        round(
          col("active_users") /
            col("cohort_size") * 100,
          2
        )
      )

      // ----------------------------------------------------------
      // BONUS : REVENU MOYEN PAR UTILISATEUR
      // ----------------------------------------------------------

      .withColumn(
        "revenue_per_user",
        round(
          col("revenue") /
            col("active_users"),
          2
        )
      )
      .select(
        "cohort_month",
        "period_index",
        "cohort_size",
        "active_users",
        "retention_rate",
        "revenue",
        "revenue_per_user"
      )
      .orderBy(
        col("cohort_month"),
        col("period_index")
      )
  }


  /**
   * Identifie la cohorte ayant la meilleure rétention
   * à 3 mois.
   *
   * period_index = 3 correspond au troisième mois après
   * le mois initial de la cohorte.
   */
  def bestCohortAt3Months(
      cohortData: DataFrame
  ): DataFrame = {

    cohortData
      .filter(
        col("period_index") === 3
      )
      .orderBy(
        col("retention_rate").desc
      )
      .limit(1)
  }


  // ============================================================
  // QUESTION 4.3 - SEGMENTATION RFM - BONUS
  // ============================================================

  /**
   * Segmentation RFM des utilisateurs.
   *
   * R = Recency
   * F = Frequency
   * M = Monetary
   *
   * Les scores sont calculés avec ntile(5).
   */
  def analyzeRFM(
      transactions: DataFrame,
      users: DataFrame
  ): DataFrame = {

    // ------------------------------------------------------------
    // 1. PRÉPARATION DES DATES
    // ------------------------------------------------------------

    val prepared =
      transactions
        .withColumn(
          "transaction_datetime",
          to_timestamp(
            substring(
              col("timestamp"),
              1,
              14
            ),
            "yyyyMMddHHmmss"
          )
        )
        .filter(
          col("transaction_datetime").isNotNull
        )

    // ------------------------------------------------------------
    // 2. DATE DE RÉFÉRENCE
    // ------------------------------------------------------------

    val referenceDate =
      prepared
        .agg(
          max("transaction_datetime")
        )
        .first()
        .getTimestamp(0)

    // ------------------------------------------------------------
    // 3. CALCUL DES INDICATEURS RFM
    // ------------------------------------------------------------

    val rfm =
      prepared
        .groupBy("user_id")
        .agg(

          // Récence :
          // nombre de jours depuis la dernière transaction.
          datediff(
            lit(referenceDate),
            to_date(max("transaction_datetime"))
          ).alias("recency"),

          // Fréquence :
          // nombre total de transactions.
          count("*")
            .alias("frequency"),

          // Montant :
          // CA total généré.
          round(sum("amount"), 2)
            .alias("monetary")
        )

    // ------------------------------------------------------------
    // 4. CALCUL DES SCORES RFM
    // ------------------------------------------------------------

    // Pour R :
    // une faible récence est meilleure.
    val recencyWindow =
      Window
        .orderBy(
          col("recency").asc
        )

    // Pour F :
    // une fréquence élevée est meilleure.
    val frequencyWindow =
      Window
        .orderBy(
          col("frequency").asc
        )

    // Pour M :
    // un montant élevé est meilleur.
    val monetaryWindow =
      Window
        .orderBy(
          col("monetary").asc
        )

    val scored =
      rfm
        .withColumn(
          "recency_score",
          lit(6) -
            ntile(5).over(recencyWindow)
        )
        .withColumn(
          "frequency_score",
          ntile(5).over(frequencyWindow)
        )
        .withColumn(
          "monetary_score",
          ntile(5).over(monetaryWindow)
        )

    // ------------------------------------------------------------
    // 5. SEGMENTATION MÉTIER
    // ------------------------------------------------------------

    val segmented =
      scored
        .withColumn(
          "rfm_segment",

          when(
            col("recency_score") >= 4 &&
            col("frequency_score") >= 4 &&
            col("monetary_score") >= 4,
            "Champions"
          )

          .when(
            col("frequency_score") >= 4 &&
            col("monetary_score") >= 3,
            "Clients fidèles"
          )

          .when(
            col("recency_score") >= 4 &&
            col("frequency_score") <= 2,
            "Nouveaux"
          )

          .when(
            col("recency_score") <= 2 &&
            col("frequency_score") >= 3,
            "À risque"
          )

          .when(
            col("recency_score") <= 2 &&
            col("frequency_score") <= 2,
            "Perdus"
          )

          .otherwise(
            "Clients fidèles"
          )
        )

    // ------------------------------------------------------------
    // 6. CROISEMENT AVEC CUSTOMER_SEGMENT
    // ------------------------------------------------------------

    val usersSegment =
      users
        .select(
          col("user_id"),
          col("customer_segment")
        )

    // ------------------------------------------------------------
    // 7. RÉSULTAT FINAL
    // ------------------------------------------------------------

    segmented
      .join(
        usersSegment,
        Seq("user_id"),
        "left"
      )
      .select(
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
      .orderBy(
        col("user_id")
      )
  }


  /**
   * Tableau croisé entre le segment RFM calculé
   * et le customer_segment présent dans users.json.
   */
  def rfmCrossTab(
      rfmData: DataFrame
  ): DataFrame = {

    rfmData
      .groupBy(
        "rfm_segment",
        "customer_segment"
      )
      .agg(
        count("*")
          .alias("customer_count")
      )
      .orderBy(
        col("rfm_segment"),
        col("customer_segment")
      )
  }


  // ============================================================
  // QUESTION 4.4 - ANALYSE PRODUITS ET CATÉGORIES - BONUS
  // ============================================================

  /**
   * Analyse :
   *
   * 1. Top 10 produits par CA
   * 2. CA par catégorie et région
   * 3. Part de chaque catégorie dans sa région
   * 4. CA par moyen de paiement et période de la journée
   *
   * Retourne trois DataFrames.
   */
  def analyzeProductAndCategoryData(
      enrichedData: DataFrame
  ): (
      DataFrame,
      DataFrame,
      DataFrame
  ) = {

    // ------------------------------------------------------------
    // 1. TOP 10 DES PRODUITS PAR CA
    // ------------------------------------------------------------

    val topProducts =
      enrichedData
        .filter(
          col("product_id").isNotNull
        )
        .groupBy(
          col("product_id"),
          col("product_name")
        )
        .agg(

          round(
            sum("amount"),
            2
          ).alias("total_revenue"),

          round(
            avg("rating"),
            2
          ).alias("average_rating"),

          first(
            col("stock"),
            ignoreNulls = true
          ).alias("stock")
        )
        .orderBy(
          col("total_revenue").desc
        )
        .limit(10)

    // ------------------------------------------------------------
    // 2. CA PAR CATÉGORIE ET PAR RÉGION
    // ------------------------------------------------------------

    val categoryRegionRevenue =
      enrichedData
        .filter(
          col("merchant_category").isNotNull
        )
        .groupBy(
          col("merchant_category"),
          col("region")
        )
        .agg(

          round(
            sum("amount"),
            2
          ).alias("total_revenue"),

          count("*")
            .alias("transaction_count")
        )

    // ------------------------------------------------------------
    // 3. TOTAL DU CA PAR RÉGION
    // ------------------------------------------------------------

    val regionTotals =
      categoryRegionRevenue
        .groupBy("region")
        .agg(
          sum("total_revenue")
            .alias("region_total_revenue")
        )

    // ------------------------------------------------------------
    // 4. PART DE LA CATÉGORIE DANS SA RÉGION
    // ------------------------------------------------------------

    val categoryRegion =
      categoryRegionRevenue
        .join(
          regionTotals,
          Seq("region"),
          "left"
        )
        .withColumn(
          "category_share_percent",
          round(
            col("total_revenue") /
              col("region_total_revenue") * 100,
            2
          )
        )
        .select(
          "merchant_category",
          "region",
          "total_revenue",
          "transaction_count",
          "category_share_percent"
        )
        .orderBy(
          col("region"),
          col("total_revenue").desc
        )

    // ------------------------------------------------------------
    // 5. CA PAR MÉTHODE DE PAIEMENT ET PÉRIODE
    // ------------------------------------------------------------

    val paymentPeriodRevenue =
      enrichedData
        .groupBy(
          col("payment_method"),
          col("day_period")
        )
        .agg(

          round(
            sum("amount"),
            2
          ).alias("total_revenue"),

          count("*")
            .alias("transaction_count")
        )
        .orderBy(
          col("payment_method"),
          col("day_period")
        )

    // ------------------------------------------------------------
    // 6. RETOUR DES TROIS DATAFRAMES
    // ------------------------------------------------------------

    (
      topProducts,
      categoryRegion,
      paymentPeriodRevenue
    )
  }

}

package com.ecommerce.analytics

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class DataTransformation(spark: SparkSession) {

  import spark.implicits._

  /**
   * Q3.2
   *
   * Enrichit les transactions avec les informations provenant
   * des utilisateurs, produits et marchands.
   */
  def enrichTransactionData(
      transactions: DataFrame,
      users: DataFrame,
      products: DataFrame,
      merchants: DataFrame
  ): DataFrame = {

    // Alias permettant de distinguer les colonnes des différentes tables.
    val t = transactions.alias("t")
    val u = users.alias("u")
    val p = products.alias("p")
    val m = merchants.alias("m")

    /**
     * Fenêtre utilisée pour numéroter les transactions
     * d'un même utilisateur dans l'ordre chronologique.
     */
    val userWindow =
      Window
        .partitionBy("user_id")
        .orderBy("timestamp")

    /**
     * Les transactions constituent notre table principale.
     *
     * LEFT JOIN permet de conserver une transaction même si
     * l'utilisateur, le produit ou le marchand correspondant
     * n'existe pas.
     */
    t
      .join(
        u,
        col("t.user_id") === col("u.user_id"),
        "left"
      )
      .join(
        p,
        col("t.product_id") === col("p.product_id"),
        "left"
      )
      .join(
        m,
        col("t.merchant_id") === col("m.merchant_id"),
        "left"
      )

      /**
       * Sélection des colonnes utiles.
       */
      .select(
        col("t.transaction_id"),
        col("t.user_id"),
        col("t.product_id"),
        col("t.merchant_id"),
        col("t.amount"),
        col("t.timestamp"),
        col("t.location"),
        col("t.payment_method"),
        col("t.category").alias("transaction_category"),

        col("u.age"),
        col("u.annual_income"),
        col("u.city"),
        col("u.customer_segment"),

        col("p.name").alias("product_name"),
        col("p.category").alias("product_category"),
        col("p.price"),
        col("p.rating"),
        col("p.stock"),

        col("m.name").alias("merchant_name"),
        col("m.category").alias("merchant_category"),
        col("m.region"),
        col("m.commission_rate")
      )

      /**
       * Q3.1
       *
       * Application du UDF permettant d'extraire les informations
       * temporelles du timestamp.
       */
      .withColumn(
        "time_features",
        TimeFeatures.extractTimeFeaturesUDF(col("timestamp"))
      )

      // Extraction des champs produits par le UDF.
      .withColumn("hour", col("time_features.hour"))
      .withColumn("day_of_week", col("time_features.day_of_week"))
      .withColumn("month", col("time_features.month"))
      .withColumn("is_weekend", col("time_features.is_weekend"))
      .withColumn("day_period", col("time_features.day_period"))
      .withColumn(
        "is_working_hours",
        col("time_features.is_working_hours")
      )

      // Le struct temporaire n'est plus nécessaire.
      .drop("time_features")

      /**
       * Q3.2
       *
       * Rang de la transaction pour chaque utilisateur.
       *
       * Exemple :
       *
       * user A
       * transaction 1 -> rank 1
       * transaction 2 -> rank 2
       * transaction 3 -> rank 3
       */
      .withColumn(
        "transaction_rank",
        row_number().over(userWindow)
      )

      /**
       * Nombre total de transactions effectuées
       * par chaque utilisateur.
       */
      .withColumn(
        "transaction_count",
        count("*").over(
          Window.partitionBy("user_id")
        )
      )

      /**
       * Classification des utilisateurs selon leur âge.
       */
      .withColumn(
        "age_group",
        when(col("age") < 25, "Jeune")
          .when(col("age").between(26, 44), "Adulte")
          .when(col("age").between(45, 64), "Âge Moyen")
          .when(col("age") >= 65, "Senior")
          .otherwise(null)
      )
  }


  /**
   * Q3.3
   *
   * Réalise les analyses temporelles avec les Window Functions.
   *
   * Ajoute :
   *
   * 1. cumulative_amount_7d
   * 2. active_user_7d
   * 3. days_since_previous_transaction
   */
  def applyWindowAnalysis(
      enriched: DataFrame
  ): DataFrame = {

    // Le fichier contient parfois un timestamp de 16 caractères.
// Les 14 premiers correspondent au format yyyyMMddHHmmss.
// On conserve donc uniquement cette partie pour les calculs temporels.
val withDate =
  enriched.withColumn(
    "transaction_datetime",
    to_timestamp(
      substring(col("timestamp"), 1, 14),
      "yyyyMMddHHmmss"
    )
  )


    /**
     * ---------------------------------------------------------
     * 1. CUMUL DES MONTANTS SUR 7 JOURS
     * ---------------------------------------------------------
     *
     * rangeBetween travaille ici avec des secondes.
     *
     * 7 jours =
     * 7 × 24 × 60 × 60
     * = 604800 secondes
     *
     * La fenêtre va donc de :
     *
     * transaction actuelle - 7 jours
     * jusqu'à
     * transaction actuelle.
     */
    val cumulativeWindow =
      Window
        .partitionBy("user_id")
        .orderBy(
          col("transaction_datetime").cast("long")
        )
        .rangeBetween(
          -604800,
          0
        )


    /**
     * Somme des montants des transactions
     * effectuées par l'utilisateur pendant les
     * 7 derniers jours.
     */
    val withCumulativeAmount =
      withDate.withColumn(
        "cumulative_amount_7d",
        sum("amount").over(cumulativeWindow)
      )


    /**
     * ---------------------------------------------------------
     * 2. UTILISATEUR ACTIF SUR 7 JOURS
     * ---------------------------------------------------------
     *
     * On veut savoir combien de jours DISTINCTS
     * l'utilisateur a effectué une transaction
     * pendant les 7 derniers jours.
     *
     * On crée donc d'abord une colonne contenant
     * uniquement la date.
     */
    val withTransactionDate =
      withCumulativeAmount.withColumn(
        "transaction_date",
        to_date(col("transaction_datetime"))
      )


    /**
     * Une fenêtre range permet de regarder les transactions
     * des 7 derniers jours.
     *
     * IMPORTANT :
     * countDistinct() n'est pas supporté directement
     * dans une fenêtre range par Spark.
     *
     * On utilise donc collect_set pour obtenir les dates
     * distinctes, puis size pour compter ces dates.
     */
    val activeUserWindow =
      Window
        .partitionBy("user_id")
        .orderBy(
          col("transaction_datetime").cast("long")
        )
        .rangeBetween(
          -604800,
          0
        )


    val withActiveDays =
      withTransactionDate.withColumn(
        "active_days_7d",
        size(
          collect_set("transaction_date")
            .over(activeUserWindow)
        )
      )


    /**
     * Un utilisateur est considéré comme actif
     * s'il a effectué des transactions pendant
     * au moins 5 jours distincts sur les 7 derniers jours.
     *
     * 1 -> utilisateur actif
     * 0 -> utilisateur non actif
     */
    val withActiveUser =
      withActiveDays.withColumn(
        "active_user_7d",
        when(col("active_days_7d") >= 5, 1)
          .otherwise(0)
      )


    /**
     * ---------------------------------------------------------
     * 3. TEMPS DEPUIS LA TRANSACTION PRÉCÉDENTE
     * ---------------------------------------------------------
     *
     * LAG permet de récupérer la transaction précédente
     * du même utilisateur.
     */
    val previousTransactionWindow =
      Window
        .partitionBy("user_id")
        .orderBy("transaction_datetime")


    /**
     * On récupère la date de la transaction précédente.
     */
    val withPreviousTransaction =
      withActiveUser.withColumn(
        "previous_transaction_datetime",
        lag("transaction_datetime", 1)
          .over(previousTransactionWindow)
      )


    /**
     * Calcul de la différence en secondes entre
     * la transaction actuelle et la précédente.
     *
     * Puis conversion en jours.
     *
     * Pour la première transaction d'un utilisateur,
     * il n'existe aucune transaction précédente.
     * Le résultat sera donc NULL.
     */
    withPreviousTransaction
      .withColumn(
        "days_since_previous_transaction",
        when(
          col("previous_transaction_datetime").isNotNull,
          (
            unix_timestamp(col("transaction_datetime")) -
              unix_timestamp(col("previous_transaction_datetime"))
          ) / lit(86400.0)
        )
      )
      .drop("previous_transaction_datetime")
  }
}
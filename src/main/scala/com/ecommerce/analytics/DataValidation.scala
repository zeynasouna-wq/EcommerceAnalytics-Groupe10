package com.ecommerce.analytics

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import com.ecommerce.models.{Transaction, User, Product, Merchant}
import com.ecommerce.utils.ConfigLoader

class DataValidation(spark: SparkSession) {

  import spark.implicits._

  // --- Transactions : amount > 0 et timestamp bien formé (14 caractères) ---
  def validateTransactions(ds: Dataset[Transaction]): (DataFrame, DataFrame) = {
    val df = ds.toDF()

    val withReason = df.withColumn(
      "rejection_reason",
      when(col("amount").isNull || col("amount") <= 0, "amount invalide (<= 0 ou nul)")
        .when(col("timestamp").isNull || length(col("timestamp")) =!= 14, "timestamp mal formé (doit faire 14 caractères)")
        .otherwise(lit(null: String))
    )

    val valid    = withReason.filter(col("rejection_reason").isNull).drop("rejection_reason")
    val rejected = withReason.filter(col("rejection_reason").isNotNull)

    println(s"[OK] transactions : ${valid.count()} lignes valides, ${rejected.count()} lignes rejetées")
    (valid, rejected)
  }

  // --- Users : age entre 16 et 100, annual_income > 0 ---
  def validateUsers(ds: Dataset[User]): (DataFrame, DataFrame) = {
    val df = ds.toDF()
    val minAge = ConfigLoader.minAge
    val maxAge = ConfigLoader.maxAge

    val withReason = df.withColumn(
      "rejection_reason",
      when(col("age").isNull || col("age") < minAge || col("age") > maxAge, "age hors intervalle autorisé")
        .when(col("annual_income").isNull || col("annual_income") <= 0, "annual_income invalide (<= 0 ou nul)")
        .otherwise(lit(null: String))
    )

    val valid    = withReason.filter(col("rejection_reason").isNull).drop("rejection_reason")
    val rejected = withReason.filter(col("rejection_reason").isNotNull)

    println(s"[OK] users : ${valid.count()} lignes valides, ${rejected.count()} lignes rejetées")
    (valid, rejected)
  }

  // --- Products : price > 0, rating entre 1 et 5 ---
  def validateProducts(ds: Dataset[Product]): (DataFrame, DataFrame) = {
    val df = ds.toDF()
    val minRating = ConfigLoader.minRating
    val maxRating = ConfigLoader.maxRating

    val withReason = df.withColumn(
      "rejection_reason",
      when(col("price").isNull || col("price") <= 0, "price invalide (<= 0 ou nul)")
        .when(col("rating").isNull || col("rating") < minRating || col("rating") > maxRating, "rating hors intervalle autorisé")
        .otherwise(lit(null: String))
    )

    val valid    = withReason.filter(col("rejection_reason").isNull).drop("rejection_reason")
    val rejected = withReason.filter(col("rejection_reason").isNotNull)

    println(s"[OK] products : ${valid.count()} lignes valides, ${rejected.count()} lignes rejetées")
    (valid, rejected)
  }

  // --- Merchants : commission_rate entre 0 et 1 ---
  def validateMerchants(ds: Dataset[Merchant]): (DataFrame, DataFrame) = {
    val df = ds.toDF()

    val withReason = df.withColumn(
      "rejection_reason",
      when(col("commission_rate").isNull || col("commission_rate") < 0 || col("commission_rate") > 1,
        "commission_rate hors intervalle [0, 1]")
        .otherwise(lit(null: String))
    )

    val valid    = withReason.filter(col("rejection_reason").isNull).drop("rejection_reason")
    val rejected = withReason.filter(col("rejection_reason").isNotNull)

    println(s"[OK] merchants : ${valid.count()} lignes valides, ${rejected.count()} lignes rejetées")
    (valid, rejected)
  }
}
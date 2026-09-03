package com.ecommerce.analytics

import org.apache.spark.sql.SparkSession

object TestMembreB {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("TestMembreB")
      .master("local[*]")
      .getOrCreate()

    val ingestion = new DataIngestion(spark)
    val transformation = new DataTransformation(spark)

    // Lecture des données
    val transactions = ingestion.readTransactions().get.toDF()
    val users = ingestion.readUsers().get.toDF()
    val products = ingestion.readProducts().get.toDF()
    val merchants = ingestion.readMerchants().get.toDF()

    println("\n===== DONNÉES BRUTES =====")
    println(s"Transactions : ${transactions.count()}")
    println(s"Users        : ${users.count()}")
    println(s"Products     : ${products.count()}")
    println(s"Merchants    : ${merchants.count()}")

    // Transformation membre B
    val enriched =
      transformation.enrichTransactionData(
        transactions,
        users,
        products,
        merchants
      )

    // ============================================================
// Q3.3 - ANALYSE PAR FENÊTRES
// ============================================================

    val windowAnalysis =
      transformation.applyWindowAnalysis(enriched)

    println("\n===== Q3.3 - WINDOW ANALYSIS =====")

    println(
      s"Nombre de lignes après analyse : ${windowAnalysis.count()}"
    )

    println("\n===== COLONNES Q3.3 =====")

    windowAnalysis
      .select(
        "transaction_id",
        "user_id",
        "timestamp",
        "amount",
        "cumulative_amount_7d",
        "active_days_7d",
        "active_user_7d",
        "days_since_previous_transaction"
      )
      .show(20, truncate = false)

    println("\n===== DONNÉES ENRICHIES =====")
    println(s"Nombre de lignes : ${enriched.count()}")

    println("\n===== SCHEMA =====")
    enriched.printSchema()

    println("\n===== APERÇU =====")
    enriched.show(10, truncate = false)

    spark.stop()
  }
}

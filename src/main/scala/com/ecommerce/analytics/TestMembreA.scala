package com.ecommerce.analytics

import org.apache.spark.sql.SparkSession

object TestMembreA {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TestMembreA")
      .master("local[*]")
      .getOrCreate()

    val ingestion  = new DataIngestion(spark)
    val validation = new DataValidation(spark)
    val quality    = new DataQualityReport(spark)

    // On garde une référence aux datasets bruts lus pour le calcul d'intégrité
    // référentielle (Question 2.5, bonus), qui compare les transactions aux
    // référentiels users/products/merchants tels que lus (avant leur propre validation).
    val transactionsDs = ingestion.readTransactions()
    val usersDs         = ingestion.readUsers()
    val productsDs      = ingestion.readProducts()
    val merchantsDs      = ingestion.readMerchants()

    val datasetsForReport = Seq(
      transactionsDs.map(ds => ("transactions", ds.count(), validation.validateTransactions(ds))),
      usersDs.map(ds => ("users", ds.count(), validation.validateUsers(ds))),
      productsDs.map(ds => ("products", ds.count(), validation.validateProducts(ds))),
      merchantsDs.map(ds => ("merchants", ds.count(), validation.validateMerchants(ds)))
    ).flatten.map { case (name, nbLues, (valid, rejected)) => (name, nbLues, valid, rejected) }

    val referentialCounts = for {
      tx <- transactionsDs
      us <- usersDs
      pr <- productsDs
      me <- merchantsDs
    } yield quality.checkReferentialIntegrity(tx.toDF(), us.toDF(), pr.toDF(), me.toDF())

    quality.buildReport(datasetsForReport, referentialCounts)

    spark.stop()
  }
}
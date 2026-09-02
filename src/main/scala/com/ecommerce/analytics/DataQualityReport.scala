package com.ecommerce.analytics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.ecommerce.utils.ConfigLoader

// Doit être définie au niveau du package (pas à l'intérieur de la classe
// DataQualityReport) : Spark a besoin de générer un Encoder pour cette case
// class via .toDF(), et il ne sait pas le faire pour une case class imbriquée
// dans une classe (elle serait alors liée à l'instance "this" englobante,
// ce que Spark refuse : "Unable to generate an encoder for inner class").
case class QualityRow(
  dataset: String,
  nb_lignes_lues: Long,
  nb_lignes_valides: Long,
  nb_lignes_rejetees: Long,
  taux_rejet: Double,
  nb_valeurs_nulles: Long,
  // --- Bonus Question 2.5 : rempli uniquement pour la ligne "transactions",
  // laissé à None pour les autres datasets (users, products, merchants) ---
  nb_refs_user_id_inexistant: Option[Long] = None,
  nb_refs_product_id_inexistant: Option[Long] = None,
  nb_refs_merchant_id_inexistant: Option[Long] = None
)

class DataQualityReport(spark: SparkSession) {

  import spark.implicits._

  // Compte le nombre total de valeurs nulles, toutes colonnes confondues
  private def countNulls(df: DataFrame): Long = {
    val nullCounts = df.columns.map(c => sum(when(col(c).isNull, 1).otherwise(0)).alias(c))
    val row = df.select(nullCounts: _*).first()
    row.toSeq.map(_.asInstanceOf[Long]).sum
  }

  // --- Question 2.5 (BONUS) : intégrité référentielle ---
  // Compte, parmi les transactions, celles dont le user_id / product_id / merchant_id
  // ne correspond à aucun enregistrement du référentiel correspondant.
  // On travaille sur les transactions telles que lues (avant filtrage 2.2) : une
  // transaction peut être orpheline indépendamment de son montant ou de son timestamp,
  // ce sont deux problèmes de qualité distincts.
  def checkReferentialIntegrity(
    transactions: DataFrame,
    users: DataFrame,
    products: DataFrame,
    merchants: DataFrame
  ): (Long, Long, Long) = {

    val nbOrphanUsers = transactions
      .join(users.select("user_id"), Seq("user_id"), "left_anti")
      .count()

    val nbOrphanProducts = transactions
      .join(products.select("product_id"), Seq("product_id"), "left_anti")
      .count()

    val nbOrphanMerchants = transactions
      .join(merchants.select("merchant_id"), Seq("merchant_id"), "left_anti")
      .count()

    println(
      s"[Intégrité référentielle] user_id inexistant : $nbOrphanUsers | " +
      s"product_id inexistant : $nbOrphanProducts | merchant_id inexistant : $nbOrphanMerchants"
    )

    (nbOrphanUsers, nbOrphanProducts, nbOrphanMerchants)
  }

  def buildReport(
    datasets: Seq[(String, Long, DataFrame, DataFrame)],
    // (nom_dataset, nb_lignes_lues, valid, rejected)
    referentialIntegrity: Option[(Long, Long, Long)] = None
    // (nbOrphanUsers, nbOrphanProducts, nbOrphanMerchants) — calculé via checkReferentialIntegrity,
    // rattaché à la ligne "transactions" du rapport
  ): DataFrame = {

    val rows = datasets.map { case (name, nbLues, valid, rejected) =>
      val nbValid    = valid.count()
      val nbRejected = rejected.count()
      val tauxRejet  = if (nbLues == 0) 0.0 else math.round((nbRejected.toDouble / nbLues) * 10000) / 100.0

      // On recompose l'ensemble des lignes lues (valides + rejetées, sans la colonne
      // technique rejection_reason) pour compter les nulls sur TOUTES les lignes lues,
      // et pas seulement sur les lignes ayant survécu à la validation. Sinon, une colonne
      // non couverte par les règles de validation (ex : location, city) verrait ses valeurs
      // nulles sous-comptées dès qu'une ligne est rejetée pour une autre raison.
      val allReadRows = valid.unionByName(rejected.drop("rejection_reason"))
      val nbNulls      = countNulls(allReadRows)

      val (orphanUsers, orphanProducts, orphanMerchants) =
        if (name == "transactions") {
          referentialIntegrity match {
            case Some((u, p, m)) => (Some(u), Some(p), Some(m))
            case None            => (None, None, None)
          }
        } else (None, None, None)

      QualityRow(name, nbLues, nbValid, nbRejected, tauxRejet, nbNulls, orphanUsers, orphanProducts, orphanMerchants)
    }

    val report = rows.toDF()
    report.show(truncate = false)

    report.write
      .mode("overwrite")
      .option("header", "true")
      .csv(s"${ConfigLoader.outputPath}data_quality_report")

    report
  }
}
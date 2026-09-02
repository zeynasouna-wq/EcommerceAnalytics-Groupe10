package com.ecommerce.analytics

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types._
import com.ecommerce.models.{Transaction, User, Product, Merchant}
import com.ecommerce.utils.ConfigLoader
import scala.util.{Try, Success, Failure}

class DataIngestion(spark: SparkSession) {

  import spark.implicits._

  // --- 1. transactions.csv : on donne le schéma nous-mêmes ---
  def readTransactions(): Option[Dataset[Transaction]] = {
    Try {
      val schema = StructType(Seq(
        StructField("transaction_id", StringType, nullable = true),
        StructField("user_id",        StringType, nullable = true),
        StructField("product_id",     StringType, nullable = true),
        StructField("merchant_id",    StringType, nullable = true),
        StructField("amount",         DoubleType, nullable = true),
        StructField("timestamp",      StringType, nullable = true),
        StructField("location",       StringType, nullable = true),
        StructField("payment_method", StringType, nullable = true),
        StructField("category",       StringType, nullable = true)
      ))

      spark.read
        .option("header", "true")
        .schema(schema)
        .csv(ConfigLoader.transactionsPath)
        .as[Transaction]
    } match {
      case Success(ds) =>
        println(s"[OK] transactions : ${ds.count()} lignes lues")
        Some(ds)
      case Failure(e) =>
        println(s"[ERREUR] Impossible de lire transactions.csv : ${e.getMessage}")
        None
    }
  }

  // --- 2. users.json ---
  def readUsers(): Option[Dataset[User]] = {
    Try {
      // Sans schéma explicite, Spark infère "age" comme BIGINT (Long) à partir
      // du JSON, alors que la case class User attend un Int : .as[User] échoue
      // alors avec "CANNOT_UP_CAST_DATATYPE". On fixe donc le schéma nous-mêmes,
      // ce qui nous permet aussi de gérer explicitement le champ imbriqué
      // preferred_categories (tableau de chaînes).
      val schema = StructType(Seq(
        StructField("user_id",              StringType,                 nullable = true),
        StructField("age",                  IntegerType,                nullable = true),
        StructField("annual_income",        DoubleType,                 nullable = true),
        StructField("city",                 StringType,                 nullable = true),
        StructField("customer_segment",     StringType,                 nullable = true),
        StructField("preferred_categories", ArrayType(StringType, true), nullable = true),
        StructField("registration_date",    StringType,                 nullable = true)
      ))

      spark.read
        .schema(schema)
        .json(ConfigLoader.usersPath)
        .as[User]
    } match {
      case Success(ds) =>
        println(s"[OK] users : ${ds.count()} lignes lues")
        Some(ds)
      case Failure(e) =>
        println(s"[ERREUR] Impossible de lire users.json : ${e.getMessage}")
        None
    }
  }

  // --- 3. products.parquet ---
  def readProducts(): Option[Dataset[Product]] = {
    Try {
      spark.read.parquet(ConfigLoader.productsPath).as[Product]
    } match {
      case Success(ds) =>
        println(s"[OK] products : ${ds.count()} lignes lues")
        Some(ds)
      case Failure(e) =>
        println(s"[ERREUR] Impossible de lire products.parquet : ${e.getMessage}")
        None
    }
  }

  // --- 4. merchants.csv ---
  def readMerchants(): Option[Dataset[Merchant]] = {
    Try {
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(ConfigLoader.merchantsPath)
        .as[Merchant]
    } match {
      case Success(ds) =>
        println(s"[OK] merchants : ${ds.count()} lignes lues")
        Some(ds)
      case Failure(e) =>
        println(s"[ERREUR] Impossible de lire merchants.csv : ${e.getMessage}")
        None
    }
  }
}
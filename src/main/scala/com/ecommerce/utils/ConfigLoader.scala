package com.ecommerce.utils

import com.typesafe.config.{Config, ConfigFactory}

object ConfigLoader {

  // Charge automatiquement le fichier application.conf
  // (Typesafe Config le trouve tout seul dans src/main/resources)
  private val config: Config = ConfigFactory.load()

  // --- Chemins des fichiers de données ---
  def transactionsPath: String = config.getString("app.data.input.transactions")
  def usersPath: String        = config.getString("app.data.input.users")
  def productsPath: String     = config.getString("app.data.input.products")
  def merchantsPath: String    = config.getString("app.data.input.merchants")

  def outputPath: String       = config.getString("app.data.output.path")

  // --- Paramètres Spark ---
  def sparkMaster: String      = config.getString("app.spark.master")
  def shufflePartitions: Int   = config.getInt("app.spark.shuffle.partitions")

  // --- Seuils de validation ---
  def minAge: Int      = config.getInt("validation.user.min-age")
  def maxAge: Int      = config.getInt("validation.user.max-age")
  def minRating: Double = config.getDouble("validation.product.min-rating")
  def maxRating: Double = config.getDouble("validation.product.max-rating")
}
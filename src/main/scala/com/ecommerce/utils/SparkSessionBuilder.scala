package com.ecommerce.utils

import org.apache.spark.sql.SparkSession

object SparkSessionBuilder {

  def build(): SparkSession = {
    SparkSession.builder()
      .appName("EcommerceAnalytics")
      .master(ConfigLoader.sparkMaster)
      .config("spark.sql.shuffle.partitions", ConfigLoader.shufflePartitions)
      .getOrCreate()
  }
}
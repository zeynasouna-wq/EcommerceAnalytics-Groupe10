package com.ecommerce.analytics

import java.time.DayOfWeek
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.Try

import org.apache.spark.sql.Row
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._


/**
 * Extraction des caractéristiques temporelles d'une transaction.
 *
 * Le timestamp attendu est au format :
 * yyyyMMddHHmmss
 *
 * Exemple :
 * 20250715190809
 * -> 15 juillet 2025 à 19h08
 */
object TimeFeatures {

  /**
   * Formatter utilisé pour convertir le timestamp
   * en LocalDateTime.
   */
  private val formatter =
    DateTimeFormatter.ofPattern("yyyyMMddHHmmss")


  /**
   * Schéma du résultat retourné par le UDF.
   *
   * Le résultat est un StructType contenant plusieurs champs.
   */
  private val timeFeaturesSchema =
    StructType(
      Seq(
        StructField("hour", IntegerType, nullable = true),
        StructField("day_of_week", StringType, nullable = true),
        StructField("month", StringType, nullable = true),
        StructField("is_weekend", IntegerType, nullable = true),
        StructField("day_period", StringType, nullable = true),
        StructField("is_working_hours", IntegerType, nullable = true)
      )
    )


  /**
   * Essaie de convertir le timestamp en LocalDateTime.
   *
   * Si le timestamp est :
   * - null
   * - vide
   * - mal formé
   *
   * la fonction retourne None au lieu de provoquer
   * une erreur.
   */
  private def parseTimestamp(
      timestamp: String
  ): Option[LocalDateTime] = {

    if (timestamp == null || timestamp.isEmpty) {

      None

    } else {

      Try {
        LocalDateTime.parse(timestamp, formatter)
      }.toOption
    }
  }


  /**
   * Fonction principale d'extraction des caractéristiques
   * temporelles.
   */
  def extractTimeFeatures(timestamp: String): Row = {

    parseTimestamp(timestamp) match {

      case Some(dateTime) =>

        // Heure comprise entre 0 et 23.
        val hour = dateTime.getHour

        // Jour de la semaine.
        val dayOfWeek = dateTime.getDayOfWeek

        // Mois.
        val month = dateTime.getMonth


        /**
         * Samedi ou dimanche -> 1
         * Autrement -> 0
         */
        val isWeekend =
          if (
            dayOfWeek == DayOfWeek.SATURDAY ||
            dayOfWeek == DayOfWeek.SUNDAY
          ) {
            1
          } else {
            0
          }


        /**
         * Détermination de la période de la journée :
         *
         * 06h - 12h -> Morning
         * 12h - 18h -> Afternoon
         * 18h - 22h -> Evening
         * 22h - 06h -> Night
         */
        val dayPeriod =
          if (hour >= 6 && hour < 12) {

            "Morning"

          } else if (hour >= 12 && hour < 18) {

            "Afternoon"

          } else if (hour >= 18 && hour < 22) {

            "Evening"

          } else {

            "Night"
          }


        /**
         * Heures de travail :
         *
         * 09h - 17h -> 1
         * Sinon -> 0
         */
        val isWorkingHours =
          if (hour >= 9 && hour <= 17) {
            1
          } else {
            0
          }


        /**
         * Le Row respecte exactement l'ordre
         * défini dans timeFeaturesSchema.
         */
        Row(
          hour,
          dayOfWeek.toString,
          month.toString,
          isWeekend,
          dayPeriod,
          isWorkingHours
        )


      case None =>

        // Timestamp invalide -> résultat null.
        null
    }
  }


  /**
   * UDF Spark.
   *
   * On utilise l'API Java UDF1 recommandée par Spark
   * afin d'éviter l'erreur UNTYPED_SCALA_UDF.
   *
   * UDF1[String, Row] signifie :
   *
   * String -> type d'entrée
   * Row    -> type de sortie
   */
  val extractTimeFeaturesUDF =
    udf(
      new UDF1[String, Row] {

        override def call(timestamp: String): Row = {
          extractTimeFeatures(timestamp)
        }

      },
      timeFeaturesSchema
    )
}
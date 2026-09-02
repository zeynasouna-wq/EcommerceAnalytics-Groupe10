ThisBuild / version      := "0.1.0"
ThisBuild / scalaVersion := "2.12.18"   // version compatible avec Spark 3.5.x

val sparkVersion = "3.5.1"

lazy val root = (project in file("."))
  .settings(
    name := "EcommerceAnalytics",

    libraryDependencies ++= Seq(
      // --- Spark ---
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql"  % sparkVersion % Provided,

      // --- Configuration externalisée (application.conf) ---
      "com.typesafe" % "config" % "1.4.3",

      // --- Tests ---
      "org.scalatest" %% "scalatest" % "3.2.18" % Test
    ),

    // Évite les conflits de version de certaines libs transitives (courant avec Spark)
    dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2"
    ),

    // Spark 3.5.x accède, via réflexion, à des classes internes du JDK
    // (sun.nio.ch, java.nio, etc.) que le système de modules de Java 9+
    // bloque par défaut. Sans ces flags "--add-opens", on obtient une
    // IllegalAccessError au démarrage de SparkContext dès Java 17.
    // "fork := true" est indispensable : sans lui, javaOptions n'a aucun
    // effet, car le code tournerait dans la JVM de SBT lui-même plutôt
    // que dans une JVM séparée qui applique ces options.
    fork := true,
    javaOptions ++= Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED"
    )
  )

// --- Génération du JAR exécutable (sbt-assembly) ---
// Spark est fourni par le cluster/spark-submit -> scope "Provided" ci-dessus,
// donc le JAR d'assembly ne contient pas Spark lui-même (JAR plus léger).
assembly / assemblyJarName := "EcommerceAnalytics.jar"

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf"              => MergeStrategy.concat
  case "application.conf"            => MergeStrategy.concat
  case _                              => MergeStrategy.first
}

// Point d'entrée de l'application (Membre C - Partie 6)
Compile / mainClass := Some("com.ecommerce.analytics.MainApp")
assembly / mainClass := Some("com.ecommerce.analytics.MainApp")
Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated

// Même correctif que ci-dessus, mais pour "sbt runMain" (utilisé pour lancer
// TestMembreA.scala) : sans ça, SBT exclut les dépendances "Provided" (donc
// Spark) du classpath d'exécution locale, et on obtient NoClassDefFoundError
// même si le code compile parfaitement.
Compile / runMain := Defaults.runMainTask(Compile / fullClasspath, Compile / run / runner).evaluated



name := "scalaSparkEx"

version := "0.1"

scalaVersion := "2.12.11"

libraryDependencies++=Seq("org.apache.calcite"%"calcite-core"%"1.22.0","org.apache.spark" %% "spark-sql" % "3.0.1","org.antlr" % "antlr4-runtime" % "4.6",
  "org.antlr" % "stringtemplate" % "3.2","org.apache.spark" %% "spark-core" % "3.0.1")
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core_2.12" % "2.11.0"

//libraryDependencies += "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % "2.11.0"

//dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % "2.9.10"

dependencyOverrides += "com.google.guava" % "guava" % "15.0"
libraryDependencies += "com.typesafe.play"     %% "play-json"           % "2.6.13"
libraryDependencies += "com.softwaremill.sttp.client" %% "play-json" % "2.1.2"

assemblyJarName := "scalaSparkEx.jar"
mainClass in assembly := Some("MainApplication.class")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
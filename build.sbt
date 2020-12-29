name := "ScalaSparkDemo"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies += "org.apache.spark" % "spark-core_2.12" % "3.0.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.12" % "3.0.1"
libraryDependencies += "org.apache.kafka" %% "kafka" % "2.6.0"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.12" % "3.0.1"
name := "StockPatternStream"

version := "0.1"

scalaVersion := "2.11.12"


libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.5" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.5" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.5" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.5" % "provided"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.4.1" % "provided"
libraryDependencies += "org.apache.kafka" %% "kafka" % "2.4.1" % "provided"




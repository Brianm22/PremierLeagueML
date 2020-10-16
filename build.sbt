name := "PremierLeagueData"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "net.ruippeixotog" %% "scala-scraper" % sparkVersion

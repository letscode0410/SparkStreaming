ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"
autoScalaLibrary:=false
val sparkVersion ="3.2.0"

val sparkDependencies =Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion





)

libraryDependencies ++=sparkDependencies



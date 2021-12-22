ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "technical-test"
  )
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.3",
  "org.apache.spark" %% "spark-sql" % "2.4.3" % "provided",
//  "org.apache.hadoop" % "hadoop-cloud-storage" % "3.2.0",
//  "org.apache.hadoop" % "hadoop-aws" % "3.2.0",
//  "org.apache.hadoop" % "hadoop-common" % "3.2.0"
)


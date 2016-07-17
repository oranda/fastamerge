import sbt._

name := "fastamerge"

version := "0.1"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.2",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)

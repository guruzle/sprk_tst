name := "PixelLogProcessor"

version := "1.0"

scalaVersion := "2.10.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.1",
  "org.apache.hadoop" % "hadoop-aws" % "2.7.1",
  "org.scalatest" %% "scalatest" % "2.2.0" % "test"
)
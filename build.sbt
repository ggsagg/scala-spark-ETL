name := "sparkers"

version := "0.1"

scalaVersion := "2.12.14"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.3",
  "org.apache.spark" %% "spark-sql" % "2.4.3",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "org.mockito" %% "mockito-scala" % "1.16.23" % Test
)
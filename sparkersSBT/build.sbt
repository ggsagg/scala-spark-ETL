name := "sparkers"

version := "0.1"

scalaVersion := "2.12.14"

trapExit := false

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.3",
  "org.apache.spark" %% "spark-sql" % "2.4.3",
  "org.mockito" %% "mockito-scala" % "1.16.23" % Test,
  "org.scalameta" %% "munit" % "0.7.26" % Test,
  "com.github.mrpowers" %% "spark-fast-tests" % "0.23.0" % "test"
)
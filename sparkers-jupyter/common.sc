import $ivy.`org.apache.spark::spark-sql:2.4.5` 
import $ivy.`sh.almond::almond-spark:0.6.0`

import org.apache.spark.sql.{SparkSession}
val spark =
    SparkSession
      .builder()
      .appName("Scala ETL")
      .master("local[*]")
      .getOrCreate()

import spark.implicits._

def run[A](code: => A): A = {
    val start = System.currentTimeMillis()
    val res = code
    println(s"Took ${System.currentTimeMillis() - start}")
    res
}
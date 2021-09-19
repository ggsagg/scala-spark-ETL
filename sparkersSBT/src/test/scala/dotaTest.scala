import com.github.mrpowers.spark.fast.tests._
import dota.Main._
import org.apache.spark.sql.{DataFrame, SparkSession}

class dotaTest extends munit.FunSuite with DatasetComparer{

  val spark =
    SparkSession
      .builder()
      .appName("Scala ETL")
      .master("local[*]")
      .getOrCreate()

  def GetUrlContentJson2(url: String): DataFrame = {
    val result = scala.io.Source.fromURL(url).mkString
    val jsonResponseOneLine = result.toString().stripLineEnd
    val jsonRdd = spark.sparkContext.parallelize(jsonResponseOneLine :: Nil)
    spark.read.json(jsonRdd)
  }

  val dotaPlayer = GetUrlContentJson2("https://api.opendota.com/api/players/639740/recentMatches")

  test("Player matches as a list"){
    assertEquals(getPlayerMatches(dotaPlayer,5),List("6135452599","6135408545","6134018090","6127211062","6126666493"))
  }

//  test("Average KDA computation returns float"){
//    assertEqualsFloat(minKdaComputation(dotaPlayer,5),0.5.toFloat,0.01.toFloat)
//  }
//
//  test("kpcomputation"){
//    assertEqualsFloat(KPComputationTeam(GetUrlContentJson2("https://api.opendota.com/api/matches/6135452599"),dotaPlayer),50.0.toFloat,0.1.toFloat)
//  }

}



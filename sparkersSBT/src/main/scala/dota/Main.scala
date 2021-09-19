package dota

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType, _}
import org.apache.spark.sql.{DataFrame, SparkSession, _}

import scala.util.Try

object Main extends App {

  var NMATCHES = 0
  val ACCOUNT_ID = "639740" //Global variable in case the account ID should be passed as an argument
  val URL_PLAYER = "https://api.opendota.com/api/players/" ++ ACCOUNT_ID ++ "/recentMatches"

  if(args.length == 0){
    NMATCHES = 10
  } else if (args.length > 1){
      Console.err.println("Only one argument is allowed. Specify number of matches to be evaluated")
      sys.exit(1)
  } else if((Try(args(0).toInt).isFailure) || (args(0).toInt < 1)){
      Console.err.println("Only a positive number is allowed.")
      sys.exit(1)
  } else NMATCHES = args(0).toInt

  val spark =
    SparkSession
      .builder()
      .appName("Scala ETL")
      .master("local[*]")
      .getOrCreate()

  import org.apache.log4j.{Level, Logger}
  import spark.implicits._
  Logger.getRootLogger().setLevel(Level.ERROR)

  /** ******************* EXTRACT ******************** */

  //Using scala.io as the endpoint is a simple URL
  def GetUrlContentJson(url: String): DataFrame = {
    val result = scala.io.Source.fromURL(url).mkString
    val jsonResponseOneLine = result.toString().stripLineEnd
    val jsonRdd = spark.sparkContext.parallelize(jsonResponseOneLine :: Nil)
    spark.read.json(jsonRdd)
  }

  /******************** TRANSFORM *********************/

  //Gets all matches from a player and transforms them to a List of match IDs
  def getPlayerMatches(userDF: DataFrame ,nMatches: Int): List[String] = {
    userDF.limit(nMatches).select("match_id").rdd.map(r => r(0).toString).collect.toList
  }

  def avgKdaComputation(userDF: DataFrame, nMatches: Int): Float = {
    userDF
      .limit(nMatches)
      .select(
        $"kills" + $"assists" as "KA",
        $"deaths")
      .select($"KA" / $"deaths" as "KDA")
      .agg(mean("KDA"))
      .withColumn("avg(KDA)", round($"avg(KDA)", 2))
      .first()
      .getDouble(0)
      .toFloat
  }

  def maxKdaComputation(userDF: DataFrame, nMatches: Int): Float = {
    userDF
      .limit(nMatches)
      .select(
        $"kills" + $"assists" as "KA",
        $"deaths")
      .select($"KA" / $"deaths" as "KDA")
      .orderBy($"KDA".desc)
      .withColumn("KDA", round($"KDA",2))
      .first()
      .getDouble(0).toFloat
  }

  def minKdaComputation(userDF: DataFrame, nMatches: Int): Float = {
    userDF
      .limit(nMatches)
      .select(
        $"kills" + $"assists" as "KA",
        $"deaths")
      .select($"KA" / $"deaths" as "KDA")
      .orderBy($"KDA".asc)
      .withColumn("KDA", round($"KDA",2))
      .first()
      .getDouble(0).toFloat
  }

  // Computes the KP for one specific match depending on the team whether DIRE or RADIANT
  def KPComputationTeam(matchDF: DataFrame, userDF: DataFrame): Float = {
    matchDF.join(userDF, "match_id")
      .select($"kills", $"assists", $"player_slot", $"radiant_score", $"dire_score")
      .withColumn("player_slot",
        when(col("player_slot") <= 127, (($"kills" + $"assists") / ($"radiant_score")) * 100)
          .otherwise((($"kills" + $"assists") / ($"dire_score"))*100)).as("KP")
      .withColumn("player_slot",round($"player_slot",2))
      .select("player_slot").first().getDouble(0).toFloat
  }

  // Recursive method that computes the KP for all matches from a player
  def KPTotalComputation(matchesList: List[String], userDF: DataFrame): List[Float] =
    matchesList match {
      case Nil => List()
      case match_id :: rest =>
        val dotaMatchDF = GetUrlContentJson("https://api.opendota.com/api/matches/" ++ match_id)
        List(KPComputationTeam(dotaMatchDF, userDF)) ++ KPTotalComputation(rest, userDF)
    }

  def KPAvg(KPList: List[Float]): Float = {
    val x = (KPList.sum / KPList.size)
    BigDecimal.decimal(x).setScale(2, BigDecimal.RoundingMode.HALF_UP).toFloat
  }

  def KPMax(KPList: List[Float]): Float = {
    KPList.max
  }

  def KPMin(KPList: List[Float]): Float = {
    KPList.min
  }

  def createKPIdataframe(game: String, player_name: String,total_games: Int, KDAavg : Float,
                         KDAmax : Float, KDAmin : Float, KPavg : Float, KPmax : Float, KPmin : Float) : DataFrame  = {

    val KPIs = List(Row(game,player_name,total_games,KDAavg,KDAmax,KDAmin,KPavg,KPmax,KPmin))

    val schema = StructType(List(
      StructField("game", StringType, true ).withComment("The title of the game the matches are related to"),
      StructField("player_name", StringType, true).withComment("The player’s ingame name / Summoner’s name"),
      StructField("total_games", IntegerType,true),
      StructField("max_kda", FloatType, true).withComment("Maximum KDA across n games"),
      StructField("min_kda", FloatType,true).withComment("Minimum KDA across n games"),
      StructField("avg_kda", FloatType,true).withComment("Average KDA across n games"),
      StructField("max_kp", FloatType, true).withComment("Maximum KP across n games"),
      StructField("min_kp", FloatType,true).withComment("Minimum KP across n games"),
      StructField("avg_kp", FloatType,true).withComment("Average KP across n games")
    ))

    val rdd = spark.sparkContext.parallelize(KPIs)
    spark.createDataFrame(rdd,schema)
  }

  /** ******************* LOAD ******************** */

  def saveDFtoJSON(df : DataFrame, url : String) : Unit = {
    df.coalesce(1).write.mode(SaveMode.Overwrite).json(url)
  }

  /** ******************* COMPUTE ******************** */

  //Extract Data from URL
  def dotaPlayerDF = GetUrlContentJson(URL_PLAYER)
  def playerMatches = getPlayerMatches(dotaPlayerDF, NMATCHES)

  //Transform the Data and compute the KPIs
  val totalKP = KPTotalComputation(playerMatches, dotaPlayerDF)

  val maxKDA = maxKdaComputation(dotaPlayerDF, NMATCHES)
  val minKDA = minKdaComputation(dotaPlayerDF, NMATCHES)
  val avgKDA = avgKdaComputation(dotaPlayerDF, NMATCHES)

  val maxKP = KPMax(totalKP)
  val minKP = KPMin(totalKP)
  val avgKP = KPAvg(totalKP)

  val finalDF = createKPIdataframe("Dota","YrikGood",NMATCHES,maxKDA,minKDA,avgKDA,maxKP,minKP,avgKP)

  finalDF.show()

  //Save new Data as JSON
  saveDFtoJSON(finalDF,"src/main/scala/output/")

  spark.stop()
}
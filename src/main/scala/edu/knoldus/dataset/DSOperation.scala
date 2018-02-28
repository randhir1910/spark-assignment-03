package edu.knoldus.dataset

import edu.knoldus.model.{Constant, Football, WinnerTeam}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object DSOperation {
  Logger.getLogger("org").setLevel(Level.OFF)
  val conf: SparkConf = new SparkConf().setMaster("local[*]")
  val spark: SparkSession = SparkSession.builder().appName("Spark assignment").config(conf).getOrCreate()

  import spark.implicits._

  val csvFileData: DataFrame = spark.read.format("csv").option("header", "true").load(Constant.csvPath)
  val footballData: DataFrame = csvFileData.select("HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR").toDF("HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR")

  def getFootballData: Dataset[Football] = {
    footballData.map(row => Football(row.getAs[String]("HomeTeam"), row.getAs[String]("AwayTeam"), row.getAs[String]("FTHG").toInt,
      row.getAs[String]("FTAG").toInt, row.getAs[String]("FTR")))
  }

  def getTotalMatchPlayed: Dataset[Row] = {
    getFootballData.select($"HomeTeam").union(getFootballData.select($"AwayTeam")).groupBy($"HomeTeam").count()
  }

  def getTopTenTeam: Dataset[WinnerTeam] = {
    val homeTeam = getFootballData.select("HomeTeam", "FTR").where("FTR = 'H'").groupBy("HomeTeam").count().withColumnRenamed("count", "HomeTeamWins")
    val awayTeam = getFootballData.select("AwayTeam", "FTR").where("FTR = 'A'").groupBy("AwayTeam").count().withColumnRenamed("count", "AwayTeamWins")
    val teams = homeTeam.join(awayTeam, homeTeam.col("HomeTeam") === awayTeam.col("AwayTeam"))
    val sum: (Int, Int) => Int = (HomeMatches: Int, TeamMatches: Int) => HomeMatches + TeamMatches
    val total = udf(sum)
    teams.withColumn("TotalWin", total(col("HomeTeamWins"), col("AwayTeamWins"))).select("HomeTeam", "TotalWin")
      .withColumnRenamed("HomeTeam", "Team").sort(desc("TotalWin")).limit(Constant.TEN).as[WinnerTeam]
  }
}

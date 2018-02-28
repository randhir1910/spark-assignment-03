package edu.knoldus.dataframe

import edu.knoldus.model.Constant
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object DFOperation {

  val conf: SparkConf = new SparkConf().setMaster("local[*]")
  val spark: SparkSession = SparkSession.builder().appName("Spark assignment").config(conf).getOrCreate()
  val csvFileData: DataFrame = spark.read.format("csv").option("header", "true").load(Constant.csvPath)
  val footballData: DataFrame = csvFileData.select("HomeTeam", "AwayTeam", "FTR").toDF("HomeTeam", "AwayTeam", "FTR")
  csvFileData.createOrReplaceTempView("footballDataView")
  val homeTeamWin: DataFrame = spark.sql("select HomeTeam,sum(case when FTR = 'H' then 1 else 0 end) as homeTeamWins," +
    "count(*) as homeTeamPlayed from footballDataView  group by HomeTeam")
  val awayTeamWin: DataFrame = spark.sql("select AwayTeam,sum(case when FTR = 'A' then 1 else 0 end) as awayTeamWins," +
    "count(*) as awayTeamPlayed from footballDataView   group by AwayTeam")
  homeTeamWin.createOrReplaceTempView("homeTeamWinnerView")
  awayTeamWin.createOrReplaceTempView("awayTeamWinnerView")

  def getDataFromFile: DataFrame = {
    csvFileData
  }

  def getHomeTeamMatchCount: DataFrame = {
    spark.sql("select HomeTeam as TeamName, count(*) as matchPlayed from footballDataView group by HomeTeam")
  }

  def getTeamWinPercentage: DataFrame = {
    spark.sql("select HomeTeam, (homeTeamWins + awayTeamWins)*100/(homeTeamPlayed + awayTeamPlayed) as winner from " +
      "homeTeamWinnerView  join awayTeamWinnerView  on HomeTeam = AwayTeam group by HomeTeam,winner order by winner desc limit 10")
  }
}

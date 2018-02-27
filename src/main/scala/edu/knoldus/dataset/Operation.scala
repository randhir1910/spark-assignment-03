package edu.knoldus.dataset

import edu.knoldus.model.{Constant, Football}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Operation extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  val conf: SparkConf = new SparkConf().setMaster("local[*]")
  val spark: SparkSession = SparkSession.builder().appName("Spark assignment").config(conf).getOrCreate()

  import spark.implicits._

  val csvFileData: DataFrame = spark.read.format("csv").option("header", "true").load(Constant.csvPath)
  val footballData: DataFrame = csvFileData.select("HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR").toDF("HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR")

  def getFootballData: Dataset[Football] =  {
    footballData.map(row => Football(row.getAs[String]("HomeTeam"),row.getAs[String]("AwayTeam"),row.getAs[String]("FTHG").toInt,
      row.getAs[String]("FTAG").toInt, row.getAs[String]("FTR")))
  }

}

package edu.knoldus

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Operation extends App {

  val conf = new SparkConf().setMaster("local[*]")
  val spark = SparkSession.builder().appName("Spark assignment").config(conf).getOrCreate()
  val csvPath = "src/main/resources/D1.csv"
  val fileData: DataFrame = spark.read.format("csv").option("header","true").load(csvPath)
  fileData.createOrReplaceTempView("football")
  val sqlDF = spark.sql("select HomeTeam as TeamName,count(HomeTeam) as homeMatch from football where group by HomeTeam")

  fileData.show()
  sqlDF.show()
}

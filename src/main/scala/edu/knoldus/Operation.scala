package edu.knoldus

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Operation extends App {

  val conf = new SparkConf().setMaster("local[*]")
  val spark = SparkSession.builder().appName("Spark assignment").config(conf).getOrCreate()
  val csvPath = "src/main/resources/D1.csv"
  val fileData: DataFrame = spark.read.csv(csvPath).toDF()
  fileData.createOrReplaceTempView("football")
  val sqlDF = spark.sql("select _c2 as team ,count(_c2) as homeMatchCount from football where _c2 <>'HomeTeam' group by _c2")
  // val homeTeamWin = spark.sql("select count(_6) as homeWin from football where _c6 ='H' group by _c2")

  fileData.show()
  sqlDF.show()
}

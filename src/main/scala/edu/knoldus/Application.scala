package edu.knoldus

import edu.knoldus.dataframe.Operation
import org.apache.log4j.{Level, Logger}


object Application {
  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger(this.getClass)
    Logger.getLogger("org").setLevel(Level.OFF)
     Operation.getDataFromFile.show ()
     logger.info("\n\n")
     Operation.getHomeTeamMatchCount.show()
     logger.info("\n\n")
     Operation.homeTeamWin.show()
      logger.info("\n\n")
      Operation.getTopTenTeam.show()
  }
}

package edu.knoldus

import edu.knoldus.dataframe.DFOperation
import edu.knoldus.dataset.DSOperation
import org.apache.log4j.{Level, Logger}


object Application {
  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger(this.getClass)
    Logger.getLogger("org").setLevel(Level.OFF)
    DFOperation.getDataFromFile.show()
    logger.info("\n\n")
    DFOperation.getHomeTeamMatchCount.show()
    logger.info("\n\n")
    DFOperation.homeTeamWin.show()
    logger.info("\n\n")
    DFOperation.getTeamWinPercentage.show()
    logger.info("\n\n")
    DSOperation.getFootballData.show()
    logger.info("\n\n")
    DSOperation.getTotalMatchPlayed.show()
    logger.info("\n\n")
    DSOperation.getTopTenTeam.show()

  }
}

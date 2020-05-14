package com.bigdata

import org.apache.spark.sql.{DataFrame, SparkSession}


/**
 * 프로젝트 공통 유틸리티 함수 모음
 * 2020.05.13 by dhkim
 */

object CommonUtils {

  /**
   * SparkStreaming에서 broadcast할 변수들 담을 케이스 클래스
   * @param histSize: historical size
   * @param rtSize: real-time size
   * @param batchInterval: streaming interval
   * @param outputDir: save dir
   */
  case class BroadcastItems(histSize: Int, rtSize: Int, batchInterval: Int, outputDir: String) {}


  def getHistPriceMap(spark: SparkSession, rawPriceDf: DataFrame, priceSize: Int,
                      dates: Array[String]): Map[String, Map[String, Double]] = {
    import spark.implicits._

    // Map[symb -> Map[date -> price]]
    val priceMap = rawPriceDf
      .map{ row =>
        val symb = row.getString(priceSize)
        val prices = (0 until priceSize).map(row.getLong(_).toDouble)
        (symb, dates.zip(prices).toMap)
      }
      .collect
      .toMap

    priceMap
  }

  def getRtPriceMap(spark: SparkSession, rawPriceDf: DataFrame, priceSize: Int): Map[String, Seq[Double]] = {
    import spark.implicits._

    val priceMap = rawPriceDf
      .map{ row =>
        val symb = row.getString(priceSize)
        val prices = (0 until priceSize).map(row.getLong(_).toDouble)
        (symb, prices)
      }
      .collect
      .toMap

    priceMap
  }

  def getSymb2NameMap(spark: SparkSession, path: String): Map[String, String] = {
    import spark.implicits._

    val symb2nameMapDf: DataFrame = spark.read.json(path)
    val symb2nameMap: Map[String, String] = symb2nameMapDf
      .map { r =>
        val symbCol = r.getAs[String]("symb")
        val nameCol = r.getAs[String]("name")
        (symbCol, nameCol)
      }
      .collect
      .toMap

    // symb to name Map
    symb2nameMap
  }

  def getStockSymbols(spark: SparkSession, rawHistDf: DataFrame): Array[String] = {
    import spark.implicits._

    val stockSymbols = rawHistDf
      .select($"symb")
      .collect
      .map(_(0).toString)

    stockSymbols
  }

}

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

  /**
   * argument parse
   * @param map: scala Map
   * @param argList: argument 문자열 리스트
   * @return 키-밸류 Map
   */
  @scala.annotation.tailrec
  def parseArgs(map : Map[Symbol, Any], argList: List[String]) : Map[Symbol, Any] = {
    argList match {
      case Nil => map
      case "--hist-path" :: value :: tail => parseArgs(map ++ Map('histpath -> value.toString), tail)
      case "--symb2name-path" :: value :: tail => parseArgs(map ++ Map('symb2name -> value.toString), tail)
      case "--output-dir" :: value :: tail => parseArgs(map ++ Map('outputdir -> value.toString), tail)
      case "--kafka-bootstrap-server" :: value :: tail => parseArgs(map ++ Map('bootstrap -> value.toString), tail)
      case "--kafka-group-id" :: value :: tail => parseArgs(map ++ Map('groupid -> value.toString), tail)
      case "--kafka-topic" :: value :: tail => parseArgs(map ++ Map('topic -> value.toString), tail)
      case "--hist-size" :: value :: tail => parseArgs(map ++ Map('histsize -> value.toInt), tail)
      case "--rt-size" :: value :: tail => parseArgs(map ++ Map('rtsize -> value.toInt), tail)
      case "--batch-interval" :: value :: tail => parseArgs(map ++ Map('batchinterval -> value.toInt), tail)
      case option :: tail => println("Unknown argument " + option)
        sys.exit(1)
    }
  }

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

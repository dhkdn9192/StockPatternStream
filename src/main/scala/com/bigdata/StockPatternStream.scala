package com.bigdata

import org.apache.spark.sql.SparkSession
import com.bigdata.CommonUtils.BroadcastItems


/**
 * 프로젝트 메인
 * 2020.05.13 by dhkim
 */

object StockPatternStream {

  def main(args: Array[String]): Unit = {

    val usage =
      """Usage: spark-submit jarfile --hist-path [path1] --symb2name-path [path2] --output-dir [path3]
        |--kafka-bootstrap-server [addr] --kafka-group-id [id] --kafka-topic [topic]""".stripMargin

    // arguments parsing
    if (args.length == 0) println(usage)
    val env = CommonUtils.parseArgs(Map(), args.toList)
    val histPath = env.getOrElse('histpath, "/data/ailabHome/elasticHome/historyPattern/histQuotes.json").toString
    val symb2namePath = env.getOrElse('symb2name, "/data/ailabHome/elasticHome/historyPattern/symb2nameDic.json").toString
    val outputDir = env.getOrElse('outputdir, "/data/ailabHome/elasticHome/historyPattern/correlation").toString
    val kafkaBootstrapServers = env.getOrElse('bootstrap, "localhost:9092").toString
    val kafkaGroupId = env.getOrElse('groupid, "group01").toString
    val topic = env.getOrElse('topic, "topicA").toString
    val topics = Array(topic)
    val histSize = env.getOrElse('histsize, 629).asInstanceOf[Int]  // historical 데이터의 일자 수 (630 - 1)
    val rtSize = env.getOrElse('rtsize, 59).asInstanceOf[Int]  // real-time 데이터의 일자 수 (DStream의 각 rdd 일자 수) (60 - 1)
    val batchInterval = env.getOrElse('batchinterval, 60 * 5).asInstanceOf[Int]  // Spark Streaming 배치 간격

    // create spark session
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("StockPatternStream")
      .getOrCreate()

    // prepare data
    val rawHistDf = Preprocessor.getRawDf(spark, histPath)
    val scaledHistDf = Preprocessor.getScaledHistDf(spark, rawHistDf, histSize, rtSize)
    val histDates = rawHistDf.columns.slice(0, histSize)
    val stockSymbols = CommonUtils.getStockSymbols(spark, rawHistDf)
    val histPriceMap = CommonUtils.getHistPriceMap(spark, rawHistDf, histSize, histDates)
    val symb2nameMap = CommonUtils.getSymb2NameMap(spark, symb2namePath)
    val broadcastItems = BroadcastItems(histSize, rtSize, batchInterval, outputDir)

    // broadcasting
    val bcHistDates = spark.sparkContext.broadcast(histDates)
    val bcStockSymbols = spark.sparkContext.broadcast(stockSymbols)
    val bcHistPriceMap = spark.sparkContext.broadcast(histPriceMap)
    val bcSymb2nameMap = spark.sparkContext.broadcast(symb2nameMap)
    val bcBroadcastItems = spark.sparkContext.broadcast(broadcastItems)

    // spark streaming
    StreamingManager.process(spark,
      kafkaBootstrapServers,
      kafkaGroupId,
      topics,
      scaledHistDf,
      bcStockSymbols,
      bcHistDates,
      bcSymb2nameMap,
      bcHistPriceMap,
      bcBroadcastItems)
  }

}

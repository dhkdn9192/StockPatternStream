package com.bigdata

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import com.bigdata.CommonUtils.BroadcastItems


/**
 * Spark Streaming 구현
 * 2020.05.13 by dhkim
 */

object StreamingManager {

  /**
   * SparkStreaming이 수행되는 프로세스
   * @param spark: SparkSession
   * @param kafkaBootstrapServers: kafka bootstrap server addr
   * @param kafkaGroupId: kafka consumer group id
   * @param topics: kafka topics
   * @param scaledHistDf: 전처리된 historical price df
   * @param bcStockSymbols: 종목코드 배열
   * @param bcHistDates: 날짜 배열
   * @param bcSymb2nameMap: 종목코드->이름 맵
   * @param bcHistPriceMap: 종목코드,날짜->주가 맵
   * @param broadcastItems: BroadcastItems(histSize: Int, rtSize: Int, batchInterval: Int, saveDir: String)
   */
  def process(spark: SparkSession,
              kafkaBootstrapServers: String,
              kafkaGroupId: String,
              topics: Array[String],
              scaledHistDf: DataFrame,
              bcStockSymbols: Broadcast[Array[String]],
              bcHistDates: Broadcast[Array[String]],
              bcSymb2nameMap: Broadcast[Map[String, String]],
              bcHistPriceMap: Broadcast[Map[String, Map[String, Double]]],
              broadcastItems: Broadcast[BroadcastItems]): Unit = {

    import spark.implicits._

    // get broadcasted items
    val stockSymbols = bcStockSymbols.value
    val histDates = bcHistDates.value
    val symb2nameMap = bcSymb2nameMap.value
    val histPriceMap = bcHistPriceMap.value
    val bcItems = broadcastItems.value

    // create streaming context
    val ssc = new StreamingContext(spark.sparkContext, Seconds(bcItems.batchInterval))

    // kafka stream connection
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaBootstrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> kafkaGroupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    scaledHistDf.persist()
    /* scaledHistDf.show()
     * +----------------------+--------+
     * |scaledHist            |symb    |
     * +----------------------+--------+
     * |[[1.0,0.93,...], ...] |A123456 |
     * |[[...], ...]          |A888888 |
     * +----------------------+--------+
     */

    // build spark stream
    kafkaStream
      .map(_.value)
      .foreachRDD { rdd =>
        val rawRtDf = rdd.toDF()
        rawRtDf.persist()
        /* rawRtDf.show()
         * +--------+--------+--------+--------+
         * |20200401|20200402|...     |symb    |
         * +--------+--------+--------+--------+
         * |77.0    |78.0    |...     |A123456 |
         * |7.0     |7.0     |...     |A888888 |
         * +--------+--------+--------+--------+
         */

        if (!rawRtDf.head(1).isEmpty) {

          // extract dates and price dictionary
          val rtDates = rawRtDf.columns.slice(0, bcItems.rtSize)
          val lastDate = rtDates.max
          val rtPriceMap = CommonUtils.getRtPriceMap(spark, rawRtDf, bcItems.rtSize)

          // scaling rt df
          val scaledRtDf: DataFrame = Preprocessor.getScaledRtDf(spark, rawRtDf, bcItems.rtSize)
          /* scaledRtDf.show()
           * +------------------+--------+
           * |scaledRt          |symb    |
           * +------------------+--------+
           * |[0.33, 0.66, ...] |A123456 |
           * |[1.0, 1.0, ...]   |A888888 |
           * +------------------+--------+
           */

          // correlation: 각 종목별로 현재와 가장 유사했던 과거 구간을 찾은 것
          val resCorr = PatternFinder.getCorrelation(spark, scaledHistDf, scaledRtDf, stockSymbols,
            histDates, rtDates, bcItems.histSize, bcItems.rtSize)
          /* resCorr
           * Array(CorrRow(A123456, Array(20100304, 20100305, ...), 0.4773), CorrRow(...), ...)
           */

          // parsing corr items: 종목별로 찾은 과거 유사 구간을 일자별로 분리하여 각 row가 되도록 파싱 (elasticsearch에 쌓기)
          // Array((symb, name, rt date, rt price, hist date, hist price, version, similarity))
          val resTuples = resCorr
            .flatMap { corrRow =>
              // CorrRow(stkCode: String, mostSimDates: Array[String], coef: Double)
              val symb = corrRow.stkCode
              val name = symb2nameMap(symb)
              val dates = corrRow.mostSimDates
              val similarity = corrRow.coef
              val symbPriceMap = histPriceMap(symb)

              // 찾은 과거 구간을 일자별로 각 row가 되도록 쪼개기 (elasticsearch에서 일자별로 넣을 거니까)
              val parsedRows = rtDates
                .zip(rtPriceMap(symb))
                .zip(dates)
                .map { r =>
                  val rtDateCol = r._1._1
                  val rtPriceCol = r._1._2
                  val histDateCol = r._2
                  val histPriceCol = symbPriceMap(r._2)
                  (symb, name, rtDateCol, rtPriceCol, histDateCol, histPriceCol, lastDate, similarity)
                }

              parsedRows
            }

          // save output as csv file
          spark
            .sparkContext
            .parallelize(resTuples)
            .toDF()
            .write
            .format("com.databricks.spark.csv")
            .mode("append")
            .option("header", "false")
            .save(bcItems.outputDir)
        }

        rawRtDf.unpersist()
      }

    // start streaming
    ssc.start()
    ssc.awaitTermination()

    scaledHistDf.unpersist()
  }

}
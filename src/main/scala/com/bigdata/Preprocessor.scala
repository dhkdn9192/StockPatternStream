package com.bigdata

import scala.math.floor
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
 * DataFrame 로드 및 전처리
 * 2020.05.13 by dhkim
 */

object Preprocessor {

  def getRawDf(spark: SparkSession, path: String): DataFrame = {
    val rawDf = spark.read.json(path)
    rawDf
  }

  def spRound(inval: Double): Double = {
    val step = 1000000
    floor(inval*step) / step
  }

  def getScaledHistDf(spark: SparkSession, rawHistDf: DataFrame, histSize: Int, rtSize: Int): DataFrame = {
    /*
     * 윈도우 슬라이딩 및 rt 기준 슬라이드별 min-max 스케일링 적용된 historical price df 생성
     * > input
     * +--------+--------+--------+--------+
     * |20100301|20100302|...     |symb    |
     * +--------+--------+--------+--------+
     * |24.0    |22.0    |...     |A123456 |
     * |114.0   |110.0   |...     |A888888 |
     * +--------+--------+--------+--------+
     * > output
     * +----------------------+--------+
     * |scaledHist            |symb    |
     * +----------------------+--------+
     * |[[1.0,0.93,...], ...] |A123456 |
     * |[[...], ...]          |A888888 |
     * +----------------------+--------+
     */
    import spark.implicits._

    val slideSize = histSize - rtSize + 1

    val slidedHistDf = rawHistDf
      .map{ row =>
        // sliding
        val slidedHist = (0 until histSize)
          .map(row.getLong(_).toDouble)
          .toArray
          .sliding(slideSize)
          .map(_.toArray)
          .toArray

        // scaling
        val transposed = slidedHist.transpose
        val mins = transposed.map(_.min)
        val durations = transposed.map(i => i.max-i.min)  // 스케일링 기준은 slide 가 아니라, 각 slide별 같은 위치값
        val scaledAry = slidedHist.map{ elem =>
          val scaled = elem
            .indices
            .map(i => spRound((elem(i)-mins(i))/durations(i)))
            .toArray
          scaled
        }

        // symb column
        val symb = row.getString(histSize)
        (scaledAry, symb)
      }
      .toDF("scaledHist", "symb")

    slidedHistDf
  }

  def getScaledRtDf(spark: SparkSession, rawRtDf: DataFrame, rtSize: Int): DataFrame = {
    /* real-time으로 들어오는 종목별 price를 array 파싱 및 min-max 스케일ㄹ이하여 새로운 df 생성
     * > input
     * +--------+--------+--------+--------+
     * |20200401|20200402|...     |symb    |
     * +--------+--------+--------+--------+
     * |77.0    |78.0    |...     |A123456 |
     * |7.0     |7.0     |...     |A888888 |
     * +--------+--------+--------+--------+
     * > output
     * +------------------+--------+
     * |scaledRt          |symb    |
     * +------------------+--------+
     * |[0.33, 0.66, ...] |A123456 |
     * |[1.0, 1.0, ...]   |A888888 |
     * +------------------+--------+
     */
    import spark.implicits._

    val scaledRtDf = rawRtDf
      .map{ row =>
        // scaling
        val rt = (0 until rtSize).map(row.getLong(_).toDouble).toArray
        val duration = rt.max - rt.min
        val scaled = rt.map(v => spRound((v-rt.min)/duration))

        // symb column
        val symb = row.getString(rtSize)
        (scaled, symb)
      }
      .toDF("scaledRt", "symb")

    scaledRtDf
  }

}

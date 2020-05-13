package com.bigdata

import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.explode


/**
 * 종목별 과거 유사 패턴찾기 위한 Correlation 기능 구현
 * 2020.05.13 by dhkim
 */

object PatternFinder {

  /**
   *
   * @param stkCode: 종목코드
   * @param mostSimDates: 과거 구간들 중 가장 유사한 구간의 날짜값들
   * @param coef: 유사도
   */
  case class CorrRow(stkCode: String, mostSimDates: Array[String], coef: Double) {}

  /**
   * @param spark: SparkSession
   * @param scaledHistDf: window sliding 및 min-max 스케일 적용된 historical price df
   * @param scaledRtDf: min-max 스케일 적용된 real-time price df
   * @param stockSymbols: 종목코드 배열. ex) Array(A123456, A888888, ...)
   * @param histDates: scaledHistDf의 날짜들. ex) Array(20200301, 20200302, ...)
   * @param rtDates: scaledRtDf의 날짜들.
   * @param histSize: 종목별 historical price 전체 길이
   * @param rtSize: real-time으로 분석할 종가 구간 길이
   */
  def getCorrelation(spark: SparkSession, scaledHistDf: DataFrame, scaledRtDf: DataFrame, stockSymbols: Array[String],
                     histDates: Array[String], rtDates: Array[String], histSize: Int, rtSize: Int): Array[CorrRow] = {

    import spark.implicits._

    val slideSize = histSize - rtSize + 1
    val coefSize = slideSize + 1

    // 두 df를 join 및 slide들을 여러 row들로 분리(explode)
    val explodedDf = scaledRtDf.as("l")
      .join(scaledRtDf.as("r"), $"l.symb" === $"r.symb")
      .drop($"l.symb")
      .map{ row =>
        val slided = row.getAs[Seq[Seq[Double]]](0)
        val rt = row.getAs[Seq[Double]](1)
        val symb = row.getString(2)
        val combined = rt
          .zip(slided)
          .map(zipped => Seq(zipped._1) ++ zipped._2)
        (combined, symb)
      }
      .toDF("combined", "symb")
      .select(explode($"combined").as("exploded"), $"symb")  // explode 함수로 row의 slide들을 여러 row들로 분리
    /* explodedDf.show()
     * +-----------------------------+--------+
     * |exploded                     |symb    |
     * +-----------------------------+--------+
     * |[0.33, 1.0, 0.0, 0.0, ...]   |A123456 |
     * |[0.66, 0.0, 0.14, 0.16, ...] |A123456 |
     * |...                          |...     |
     * |[1.0, 0.0, 0.63, 1.0, ...]   |A888888 |
     * |[1.0, 0.2, 1.0, 0.81, ...]   |A888888 |
     * |...                          |...     |
     * +-----------------------------+--------+
     */

    // correlation 계산을 위한 벡터화
    val vectorDf = explodedDf
      .rdd
      .map{ row =>
        val vectCol = row.getAs[Seq[Double]](0).toArray
        val symb = row.getString(1)
        (Vectors.dense(vectCol), symb)
      }
      .toDF("vectors", "symb")
    /* vectorDf.show()
     * +-----------------------------+--------+
     * |vectors                      |symb    |
     * +-----------------------------+--------+
     * |[0.33, 1.0, 0.0, 0.0, ...]   |A123456 |
     * |[0.66, 0.0, 0.14, 0.16, ...] |A123456 |
     * |...                          |...     |
     * |[1.0, 0.0, 0.63, 1.0, ...]   |A888888 |
     * |[1.0, 0.2, 1.0, 0.81, ...]   |A888888 |
     * |...                          |...     |
     * +-----------------------------+--------+
     */

    val resCorrRows = stockSymbols
      .map{ symb =>
        val filteredDf = vectorDf.filter($"symb" === symb)
        val Row(coefMat: Matrix) = Correlation.corr(filteredDf, "vectors").head
        val coefAry = coefMat.toArray.slice(1, coefSize)  // 현재값 제외

        val mostSimilar = coefAry.zipWithIndex.maxBy(_._1)
        val mostSimilarDates = histDates.slice(mostSimilar._2, mostSimilar._2 + rtSize)

        // (종목코드, 상관구간 날짜들, 상관계수)
        CorrRow(symb, mostSimilarDates, mostSimilar._1)
      }
    /* resCorrRows
     * Array(CorrRow(A123456, Array(20100304, 20100305, ...), 0.4773), CorrRow(...), ...)
     */

    resCorrRows
  }

}

package com.xiaoi.spark.question

import com.xiaoi.common.Segment
import org.apache.spark.sql.SparkSession

object SimTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    val yesterdayPath = "/production/lenovo/output/unanswered_analysis/step_filter"
    val recentPath = "/production/lenovo/output/unanswered_analysis/step_recent_unans"

    import spark.implicits._

    val yesUnans = spark.read.csv(yesterdayPath).
      map(x=> x.getString(0)).
      map{case q => (q, Segment.jieba_analysis(q, " "))}.
      toDF("question","segment")
    val recentUnans = spark.read.csv(recentPath).
      map(x=> x.getString(0)).
      map{case q => (q, Segment.jieba_analysis(q, " "))}.
      toDF("question","segment")



  }
}

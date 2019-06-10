package com.xiaoi.spark.question
import com.xiaoi.common.{DateUtil, InputPathUtil}
import com.xiaoi.spark.BaseOffline
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object unansSim extends BaseOffline {
  override def process(lines: Dataset[String], params: MainBatch.Params)={
    val spark = SparkSession.builder().getOrCreate()


    val path = ""
    val date = spark.sparkContext.textFile(path)
    val av = spark.read.format("csv").load(path)


    val yesterday = InputPathUtil.getTargetDate("0")
    val paths = InputPathUtil.getInputPathArray(params.days, yesterday.plusDays(1), params.inputPath)

//    val paths = Array("/production/guangda/data/ask/2019/05/01", "/production/guangda/data/ask/2019/05/02")
    var df: DataFrame = spark.read.csv(paths(0))
    for (i <- 1 until paths.length) {
      val df_tmp = spark.read.csv(paths(i))
      df = df.union(df_tmp)
    }


  }


}

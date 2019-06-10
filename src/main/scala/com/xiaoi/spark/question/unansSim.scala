package com.xiaoi.spark.question
import com.xiaoi.common.{DateUtil, InputPathUtil}
import com.xiaoi.spark.BaseOffline
import org.apache.spark.sql.{Dataset, SparkSession}

object unansSim extends BaseOffline {
  override def process(lines: Dataset[String], params: MainBatch.Params)={
    val spark = SparkSession.builder().getOrCreate()



    val path = ""
    val date = spark.sparkContext.textFile(path)
    val av = spark.read.format("csv").load(path)

    val ac = spark.read.textFile()

    val yesterday = InputPathUtil.getTargetDate("0")
    val recent = spark.read.csv(
      InputPathUtil.getInputPath(params.days,yesterday.plusDays(1),params.inputPath)).
      cache()




  }


}

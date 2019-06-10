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
      InputPathUtil.getInputPath(params.days, yesterday.plusDays(1), params.inputPath)).
      cache()

    for (i <- 1 until paths.length) {
      val df_tmp = spark.read.format("com.databricks.spark.csv")
        .schema(customSchema)
        .option("header", hasHeader.toString)
        .option("inferSchema", "false") //是否自动推到内容的类型
        .option("delimiter", delimiter) //分隔符，默认为 ,
        .load(paths(i))
      df = df.unionAll(df_tmp)
    }


  }


}

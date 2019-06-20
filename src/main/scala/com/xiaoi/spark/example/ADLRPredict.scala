package com.xiaoi.spark.example

import java.io.{File, IOException, PrintWriter}

import com.xiaoi.common.{HDFSUtil, HadoopOpsUtil, Segment}
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import scopt.OptionParser

/**
  * ADLRPredict
  * 广告分类模型---使用spark-mllib的LogisticRegression分类器
  *
  * Created by ligz on 2016/10/31.
  */
object ADLRPredict {

  val logger = LoggerFactory.getLogger(getClass)

  def predict(params: Params) = {

    val inputPath = params.inputPath
    val corpusPath = params.corpusPath
    val modelPath = params.modelPath
    val summaryPath = params.summaryPath
    val briefPath = params.briefPath
    val topN = params.topN
    val questionIdx = params.questionIdx

    HDFSUtil.removeOrBackup(params.outPath, params.outPath)

    val conf = new SparkConf().setAppName("ad-lr-predict")
    val sc = new SparkContext(conf)

    // 加载词汇表与IDF模型
    logger.info(s"从路径[${corpusPath}]中加载词典与IDF模型")
    val corpusAndIDF = CorpusAndIDF(corpusPath)
    corpusAndIDF.load(sc)

    logger.info(s"输入数据路径：${inputPath}")
    val inputData = params.logInput match {
      case true =>  sc.textFile(inputPath).
        map(_.split("\\|")).filter(_.size > (questionIdx + 1)).map(_(questionIdx)).cache()
      case false => sc.textFile(inputPath).cache()
    }
    val inputCnt = inputData.count()
    logger.info(s"输入数据条数：$inputCnt")

    logger.info("对输入数据进行分词")
    val seg = inputData.map(x => RawDataRecord("0", x, Segment.segment(x).split(" ")))

    logger.info("进行TF-IDF转换")
    // TF-IDF特征转换
    val idfData = seg.map(item => {
      val feats = corpusAndIDF.calcTFIDF(item.words.toList)
      item.text -> LabeledPoint(item.category.toDouble, feats)
    })

    // 加载LR模型
    logger.info(s"从路径[$modelPath]加载分类模型")
    val model = LogisticRegressionModel.load(sc, modelPath)

    // 进行预测
    logger.info("使用加载好的模型进行广告判别")
    val result = idfData.map(item => item._1 -> model.predict(item._2.features).toInt).cache()

    val clsCnt = result.map(_._2).countByValue()
    val resultInfo = clsCnt.map {case (label, cnt) => s"类别：$label，个数：$cnt"}.mkString("\n")
    logger.info(s"广告判别结果信息：\n$resultInfo")

    logger.info("保存结果到HDFS中")
    result.map {case(sent, pred) => s"$sent\t$pred"}.saveAsTextFile(params.outPath)

    logger.info("广告判别结束")

    try {
      val summaryFile = new File(summaryPath)
      if (!summaryFile.getParentFile.exists()) summaryFile.getParentFile.mkdirs()
      var writer = new PrintWriter(summaryFile)
      writer.println(s"输入数据条数：$inputCnt")
      writer.println(s"广告判别结果信息：\n$resultInfo")
      writer.close()
      writer = new PrintWriter(briefPath)
      writer.println(s"结果前${topN}条：")
      result.take(topN).foreach { case(sent, pred) => writer.println(s"$sent\t$pred") }
      writer.close()
    } catch {
      case e: IOException => logger.error("保存结果概要和结果TopN时失败", e)
    }
    for(x <- 1 to 31){
      val time = "2017/7/0" + x.toString
    }
  }


  def main(args: Array[String]): Unit = {

    val defaultParams = Params()
    val parser = new OptionParser[Params]("广告判别模型参数") {
      head("广告判别模型参数列表")

      opt[String]("inputPath")
        .text("输入数据HDFS路径")
        .action((x, c) => c.copy(inputPath = x))
      opt[String]("corpusPath")
        .text("词典和IDF数据HDFS路径")
        .action((x, c) => c.copy(corpusPath = x))
      opt[String]("modelPath")
        .text("模型HDFS路径")
        .action((x, c) => c.copy(modelPath = x))
      opt[String]("outPath")
        .text("结果数据HDFS存放目录")
        .action((x, c) => c.copy(outPath = x))
      opt[String]("summaryPath")
        .text("数据概要信息本地路径")
        .action((x, c) => c.copy(summaryPath = x))
      opt[String]("briefPath")
        .text("结果TopN本地路径")
        .action((x, c) => c.copy(briefPath = x))
      opt[Int]("topN")
        .text("预览结果前N条")
        .action((x, c) => c.copy(topN = x))
      opt[Boolean]("logInput")
        .text("输入数据为日志，默认为false，代表文本")
        .action((x, c) => c.copy(logInput = x))
      opt[Int]("questionIdx")
        .text("日志中问题位置，从0开始算起，默认为3，也就是第四个位置")
        .action((x, c) => c.copy(questionIdx = x))

      checkConfig {_ => success}
    }

    parser.parse(args, defaultParams).map {
      params => predict(params)
    }.getOrElse {
      sys.exit(1)
    }

  }


  case class Params(inputPath: String = "",
                    corpusPath: String = "",
                    modelPath: String = "",
                    outPath: String = "",
                    summaryPath: String = "",
                    briefPath: String = "",
                    logInput: Boolean = false,
                    questionIdx: Int = 3,
                    topN: Int = 100)

  case class RawDataRecord(category: String, text: String, words: Seq[String])

}

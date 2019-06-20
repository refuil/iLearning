package com.xiaoi.spark.artificial_v1

import com.xiaoi.common.{HDFSUtil, InputPathUtil, Segment}
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

/**
  * 读取data/ask数据，生成fasttext训练数据
  * 将标准问和标准问ID的映射存储
  * Created by zzj on 8/28/17.
  */
object TrainFileGenerator {

  def run(params: Params): Unit = {
    val inputPath = params.inputPath
    val outputPath = params.outputPath
    val samplePath = params.samplePath
    val faqIdPath = params.faqIdPath
    val targetDate = params.targetDate
    val analyzeDays = params.analyzeDays
    val stopFilePath = params.stopFilePath
    val minimumCnt = params.minimumCnt
    val ansTypes = params.ansTypes.replaceAll("\\s+", "").split(",").toList

    val conf = new SparkConf().setAppName("TrainFileGenerator")
    val sc = new SparkContext(conf)

    HDFSUtil.removeOrBackup(outputPath, outputPath)
    HDFSUtil.removeOrBackup(samplePath, samplePath)
    HDFSUtil.removeOrBackup(faqIdPath, faqIdPath)

    // 处理一天数据或者最近n天数据
    val fromD = InputPathUtil.getTargetDate(targetDate)
    val input_data = sc.textFile(InputPathUtil.getInputPath(analyzeDays, fromD.plusDays(1), inputPath))
    //取问句，标准问，类型
    val answeredQues = input_data.map(x=>{
      val splits = x.split("\\|")
      val ques = splits(3)
      val faq = splits(8)
      val faqID = splits(7)
      val anstype = splits(6)
      (ques, faq, anstype, faqID)
    }).filter(x=>(ansTypes.contains(x._3)))

    val quesAndFaq = answeredQues.map(x=>(x._2,List(x._1))).reduceByKey(_ ++ _)
      .map(x=>{ //将标准问对应的问句统计并排序按照频次筛选TopN
        val quesCntList = x._2.groupBy(y=>y).map(y=>(y._1, y._2.length)).toList.sortBy(-_._2).map(_._1)
        val quesList = quesCntList.take(minimumCnt + quesCntList.length/100)
        (x._1, quesList)
      })

    //问句分词，去重
    val quesSegment = quesAndFaq.map(x=>{
      (x._1, x._2.filter(_.length>1))
    }).mapPartitions(x=>{
      Segment.init(true, stopFilePath, false, "")
      x
    }).map(x=>{
      val segs = x._2.map(y=>Segment.segment(y," "))
      (x._1,segs)
    }).cache
    //存储
    quesSegment.flatMapValues(x=>x)
      .map(x=>{
      "__label__"+x._1+"\t"+x._2
    }).saveAsTextFile(outputPath)
    //将标准问数量统计输出，用于查看样本分布
//    quesSegment.map(x=>(x._1,x._3)).sortBy(-_._2).saveAsTextFile(samplePath)
    //标准问和ID映射
    answeredQues.map(x=>(x._2, x._4)).distinct().map(x=>(x._1, List(x._2))).reduceByKey(_ ++ _).map(x=>(x._1, x._2(0))).map(x=> x._1 + "|" + x._2).saveAsTextFile(faqIdPath)
  }

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("TrainFileGenerator") {
      head("TrainFileGenerator")
      opt[String]('i', "inputPath")
        .text("Input path for ask data")
        .action((x, c) => c.copy(inputPath = x))
      opt[String]("outputPath")
        .text("Output path for ask data")
        .action((x, c) => c.copy(outputPath = x))
      opt[String]("samplePath")
        .text("samplePath")
        .action((x, c) => c.copy(samplePath = x))
      opt[String]("faqIdPath")
        .text("faqIdPath")
        .action((x, c) => c.copy(faqIdPath = x))
      opt[String]("targetDate")
        .text("targetDate")
        .action((x, c) => c.copy(targetDate = x))
      opt[Int]("analyzeDays")
        .text("analyzeDays")
        .action((x, c) => c.copy(analyzeDays = x))
      opt[String]("ansTypes")
        .text("ansTypes")
        .action((x, c) => c.copy(ansTypes = x))
      opt[String]("stopFilePath")
        .text("stopFilePath")
        .action((x, c) => c.copy(stopFilePath = x))
       opt[Int]("minimumCnt")
        .text("minimumCnt")
        .action((x, c) => c.copy(minimumCnt = x))
      checkConfig { params => success }
    }
    parser.parse(args, defaultParams).map{params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  case class Params(
                     inputPath: String = "",
                     outputPath: String = "",
                     samplePath: String = "",
                     faqIdPath: String = "",
                     targetDate: String = "0",
                     analyzeDays : Int = 30,
                     ansTypes : String = "1,2",
                     stopFilePath : String = "/opt/xiaoi/txt/stopwords_least.txt",
                     minimumCnt : Int = 20
                   )
}

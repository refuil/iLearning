package com.xiaoi.spark.etl_v1

import com.xiaoi.common.{FilterUtils, Segment}
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

import scala.io.Source

/**
 * created by yang.bai@xiaoi.com 2015-09-28
 * 清洗旧数据，分词
 * 1.消重
 * 2.去标点
 * 3.去全角字符
 * 4.去无用中括号
 * 5.分词
 * 6.去空格
 */
object InitOldData {

  case class Params(
    input : String = "hdfs://172.16.0.1:9000/experiment/italk/data/ask",
    toMergeOutput : String = "hdfs://172.16.0.1:9000/experiment/xiaoi_analysis/output/etl_init/step_to_merge",
    filterSegOutput : String = "",
    testOutput : String = "",
    stopFilePath: String = "/opt/xiaoi/txt/stopwords_least.txt",
    domainPath : String = "../domain_words.txt"
                   )

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("etl init") {
      head("etl init")
      opt[String]('i', "i")
        .text(s"input path")
        .action((x, c) => c.copy(input = x))
      opt[String]("stopFilePath")
        .text(s"stopFilePath path")
        .action((x, c) => c.copy(stopFilePath = x))
      opt[String]("toMergeOutput")
        .text("middle init output path")
        .action((x, c) => c.copy(toMergeOutput = x))
      opt[String]("filterSegOutput")
        .text("filterSegOutput")
        .action((x, c) => c.copy(filterSegOutput = x))
      opt[String]("testOutput")
        .text("testOutput")
        .action((x, c) => c.copy(testOutput = x))
      opt[String]("domainPath")
        .text("domainPath")
        .action((x, c) => c.copy(domainPath = x))
      checkConfig { params =>
        success
      }
    }
    parser.parse(args, defaultParams).map{params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName("etl init")
    val sc = new SparkContext(conf)
    val input = params.input
    val toMergeOutput = params.toMergeOutput
    val filterSegOutput = params.filterSegOutput
    val testOutput = params.testOutput
    val stopFilePath = params.stopFilePath
    val domainPath = params.domainPath


    //（问句，日志数组）
    val data = sc.textFile(input)
      .distinct()
      .map(_.split("\\|", -1))
      .map(x => {
      val question = x(3).replaceAll("\t", " ")
      x(3) = question
      val rb = FilterUtils.removeBracket(question)
      val rsbc = FilterUtils.removeSBC(rb)
      val question_no_punctuation = FilterUtils.replaceSymbolToSpace(rsbc)
      (x, question, question_no_punctuation)
    }).map(x => {
      (x._3, x._1)
    }).cache()

    data.saveAsObjectFile(toMergeOutput)

    //读取业务词
    val domainList = Source.fromFile(domainPath).getLines().toList.map(_.toLowerCase())
    val bc_domainList = if(domainList.size > 0) sc.broadcast(domainList) else null

    //（问句，分词）
    val filterSeg = data.map(_._1).distinct().mapPartitions(x=>{
      Segment.init(true, stopFilePath, false, "")
      if(bc_domainList != null) Segment.loadWords(bc_domainList.value)
      x
    }).map(x=>{
      val segs = Segment.segment(x)
      (x.trim, segs.trim)
    }).cache()

    filterSeg.map(x=>x._1+"\t"+x._2).saveAsTextFile(filterSegOutput)
    filterSeg.map(x=>"positive"+"\t"+x._2).saveAsTextFile(testOutput)

    filterSeg.unpersist()
    data.unpersist()

    sc.stop()
  }
}

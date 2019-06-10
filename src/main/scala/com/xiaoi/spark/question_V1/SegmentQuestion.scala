package com.xiaoi.spark.question

import com.xiaoi.common._
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

/**
  * Question segment with jieba-java
  * Created by josh on 7/20/17.
  */
object SegmentQuestion {

  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName("Segment")
    val sc = new SparkContext(conf)


    val resultRDD = sc.textFile(params.input_path)
      .map(x => {
        val list = x.split("\\|", -1)
        val unans_ques = list(0)
        val segs = getSeg(list(0), params)
        val unans_cnt = list(1)
        (unans_ques, segs)
      })
      .filter(x => x._2.length != 0)


    resultRDD
      .map(x => x._1 + "\t" + x._2)
      .saveAsTextFile(params.output_path)
    resultRDD.map(x => params.label + "\t" + x._2)
            .saveAsTextFile(params.test_path)

  }

  /**
    * Unanswer Segment method
    * @param sentence
    * @param params
    * @return
    */
  def getSeg(sentence: String, params: Params): String ={
    Segment.init(true, params.stopword_path, false, "")
    println("question: " + sentence)
    val segs = Segment.jieba_analysis(sentence, " ")
    val seg_list = segs.split(" ")
    val result = scala.collection.mutable.ListBuffer.empty[String]
    for(seg <- seg_list){
      if(params.only_chinese.toBoolean){
        if(FilterUtils.containChineseChar(seg)) result += seg
      }else
        result += seg
    }
    result.mkString(" ")
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("Count Unanswered questions count in specified period") {
      head("Count Unanswered questions Frequency")
      opt[String]('i', "input_path")
        .text("Input path for ask data")
        .action((x, c) => c.copy(input_path = x))
      opt[String]("output_path")
        .text("output")
        .action((x, c) => c.copy(output_path = x))
      opt[String]("test_path")
        .text("测试文件保存路径")
        .action((x, c) => c.copy(test_path = x))
      opt[String]("only_chinese")
        .text("是否只保留中文分词结果")
        .action((x, c) => c.copy(only_chinese = x))
      opt[String]("stopword_path")
        .text("test_path")
        .action((x, c) => c.copy(stopword_path = x))
      opt[String]("label")
        .text("待测试样本的默认类别")
        .action((x, c) => c.copy(label = x))
      checkConfig { params => success }

    }
    parser.parse(args, defaultParams).map {params =>
      if (HDFSUtil.exists(params.output_path, params.output_path)) {
        HDFSUtil.removeDir(params.output_path, params.output_path)
      }
      if (HDFSUtil.exists(params.test_path, params.test_path)) {
        HDFSUtil.removeDir(params.test_path, params.test_path)
      }
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  case class Params(
                     input_path: String = "",
                     output_path: String = "0",
                     test_path : String = "",
                     only_chinese: String = "false",
                     stopword_path: String = "",
                     label: String = ""
   )
}

package com.xiaoi.spark.etl_v1

import com.xiaoi.common.HadoopOpsUtil
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

/**
 * created by yang.bai@xiaoi.com 2015-09-28
 * 合并情感分析后的情感值
 */
object MergeOldData {
  case class Params(
    toMergeInput : String = "hdfs://172.16.0.1:9000/production/nlp/output/etl_init/step_middle_init",
    classifyInput : String = "hdfs://172.16.0.1:9000/production/nlp/output/etl_init/step_classify",
    mergeOutput : String = "hdfs://172.16.0.1:9000/production/nlp/output/etl_init/step_merge")

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("etl init") {
      head("etl init")
      opt[String]("toMergeInput")
        .text("middle init path")
        .action((x, c) => c.copy(toMergeInput = x))
      opt[String]("classify")
        .text("classify path")
        .action((x, c) => c.copy(classifyInput = x))
      opt[String]("mergeOutput")
        .text("merge reslut output path")
        .action((x, c) => c.copy(mergeOutput = x))
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
    val conf = new SparkConf().setAppName("merge for etl init")
    val sc = new SparkContext(conf)
    val toMergeInput = params.toMergeInput
    val mergeOutput = params.mergeOutput
    val classifyInput = params.classifyInput

    if(HadoopOpsUtil.exists(mergeOutput, mergeOutput))
      HadoopOpsUtil.removeDir(mergeOutput, mergeOutput)

    val classify = sc.textFile(classifyInput)
      .map(x => {
        val line = x.split("\t")
        (line(0), (line(1), line(4)))
      })
      .cache()

//  数据倾斜动态解决方案，partitionBy后做join存在数据丢失BUG
//  数据倾斜静态解决方案，key为空字符串的情况最多，单独特殊处理
    val d1 = sc.objectFile[Tuple2[String, Array[String]]](toMergeInput)
      .filter(x => x._1 != "")

    val d2 = sc.objectFile[Tuple2[String, Array[String]]](toMergeInput)
      .filter(x => x._1 == "")
      .map(x => (x._2, "", "", "0"))

//    sc.union(d1.join(classify), d2.partitionBy(new RandomPartitioner(64)).join(classify))
//    d2.partitionBy(new RandomPartitioner(64)).join(classify)
//      .union(d1.join(classify))
    d1.join(classify)
      .map(x => (x._2._1, x._1, x._2._2._1, x._2._2._2))
      .union(d2)
      .sortBy(_._1(0))
      .map(x => List(x._1.mkString("|"), x._2, x._3, x._4).mkString("|"))
      .saveAsTextFile(mergeOutput)

//    sc.objectFile[Tuple2[String, Array[String]]](toMergeInput)
//      .partitionBy(new HashPartitioner(256))
//      .join(classify)
//      .map(x => (x._2._1, x._1, x._2._2._1, x._2._2._2))
//      .sortBy(_._1(0))
//      .map(x => List(x._1.mkString("|"), x._2, x._3, x._4).mkString("|"))
//      .saveAsTextFile(mergeOutput)
    classify.unpersist()
    sc.stop()
  }
}

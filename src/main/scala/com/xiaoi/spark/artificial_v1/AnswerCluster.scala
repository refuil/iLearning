package com.xiaoi.spark.artificial_v1

import com.xiaoi.common.{CalSimilar, FilterUtils, HDFSUtil}
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser


/**
 * created by yang.bai@xiaoi.com
 * edit_dis+仿density_peak聚类
 */
object AnswerCluster {
  case class Params(
    qaPath: String = "/experiment/baiy/output/lenovo_generate_qa_pro/qa",
    output: String = "/experiment/baiy/output/lenovo_generate_qa_pro/answer_cluster",
    questionClusterPath: String = "/experiment/baiy/output/lenovo_generate_qa_pro/question_cluster",
    simThreshold: Double = 0.66,
    weightThreshold: Double = 0.0)

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("artifical answer cluster") {
      opt[String]("qaPath")
        .text("input path")
        .action((x, c) => c.copy(qaPath = x))
      opt[String]("output")
        .text("output path")
        .action((x, c) => c.copy(output = x))
      opt[String]("questionClusterPath")
        .text("question cluster path")
        .action((x, c) => c.copy(questionClusterPath = x))
      opt[Double]("simThreshold")
        .text("sim threshold")
        .action((x, c) => c.copy(simThreshold = x))
      opt[Double]("weightThreshold")
        .text("weight threshold")
        .action((x, c) => c.copy(weightThreshold = x))
    }
    parser.parse(args, defaultParams).map {
      params => run(params)
      //      params => test()
    }.getOrElse {
      sys.exit(1)
    }
  }
  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName("artifical answer cluster")
    val sc = new SparkContext(conf)

    val qaPath = params.qaPath
    val clusterPath = params.questionClusterPath
    val output = params.output
    val simThreshold = params.simThreshold
    val weightThreshold = params.weightThreshold

      HDFSUtil.removeOrBackup(output, output)

    //qa:question,answer
    val data = sc.textFile(qaPath)
      .map(_.split("\\|"))
      .map(x => (x(0), x(1)))
      .cache()
    //question_cluster:cluster_id,flag,weight,query_count,query
    val qCluster = sc.textFile(clusterPath)
      .map(_.split("\\|"))
      .map(x => (x(4), x(0)))
      .cache()

    val clusterDP = qCluster.join(data)
      .map(x => x._2)
      .map(x => (x._1, List(x._2)))
      .reduceByKey(_++_)
      .mapValues(x => {
        val sim = x.map(y => {
          val tmp = x.map(z => {
            (y, z,
              CalSimilar.calEditSimilar(
                FilterUtils.replaceSymbolToSpace(
                  FilterUtils.removeHeadAndTailSymbol(y)),
                FilterUtils.replaceSymbolToSpace(
                  FilterUtils.removeHeadAndTailSymbol(z))))
          }).sortBy(_._3).reverse
            .filter(_._3 >= simThreshold)
          (y, tmp, tmp.length)
        }).sortBy(_._3).reverse
        var cluster = List[Tuple3[Int, String, Int]]()
        var qInC = Map[String, (Int, Int)]()
        var cID = -1
        for(v <- sim){
          if(!qInC.contains(v._1)){
            var tmpID = cID + 1
            val noInC = v._2.filter(y => !qInC.contains(y._2))
            //方案1 去除已在聚类中的问句 剩余的句子作为新聚类
//            val flag = 1
//            cID += 1
            //方案2 如果相似问句已在聚类中便加入该聚类否则作为新聚类
//            val flag = if(noInC.length == v._3){
//              cID += 1
//              1
//            } else {
//              tmpID = qInC(v._2.filter(y => qInC.contains(y._2)).take(1)(0)._2)
//              0
//            }
            //方案3 句子在聚类中3个标识1为聚类内容 2为聚类边缘 3为噪音 如果相似问句已在聚类中便加入该聚类并且标识-1 否则作为新聚类
            val simC = v._2.filter(y => qInC.contains(y._2)).map(y => qInC(y._2)).sortBy(_._2).reverse.take(1)
            val flag = if(noInC.length == v._3 || simC(0)._2 != 1){
              cID += 1
              1
            } else {
              tmpID = simC(0)._1
              simC(0)._2 - 1
            }
            cluster = cluster ++ noInC.map(y => (tmpID, y._2, flag))
            qInC = qInC ++ noInC.map(x => (x._2, (tmpID, flag))).toMap
          }
        }
        cluster
      })
      .flatMap(x => x._2.map(y => (x._1, y._1, y._2, y._3)))
      .cache()
    val clusterC = clusterDP
      .map(x => ((x._1, x._2), 1))
      .reduceByKey(_+_)
      .sortBy(_._2, false)
      .cache()
    val cMaxW = (clusterC.take(1)(0)._2 - 1).toDouble
    val clusterW = clusterC.map(x => (x._1, (x._2 - 1).toDouble/cMaxW))
      .filter(x => x._2 >= weightThreshold)
    clusterDP.map(x => ((x._1, x._2), (x._3, x._4)))
      .join(clusterW)
      .map(x => List(x._1._1, x._2._2, x._2._1._1))
      .distinct()
      .sortBy(x => (x(1).toString.toDouble, x(0).toString), false)
      .map(_.mkString("|"))
      .saveAsTextFile(output)

  }
}
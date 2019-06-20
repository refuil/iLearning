package com.xiaoi.spark.artificial_v1

import com.xiaoi.common._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import scopt.OptionParser

import scala.util.Random


/**
 * created by yang.bai@xiaoi.com
 * ansj分词+tfidf+kmeans聚类分组
 * edit_dis+仿density_peak聚类
 * 根据词性对聚类结果再细分
 */
object QuestionCluster {
  case class Params(
    input: String = "/experiment/baiy/output/lenovo_generate_qa_pro/user_question",
    output: String = "/experiment/baiy/output/lenovo_generate_qa_pro/question_cluster",
    stopFilePath: String = "/opt/xiaoi/txt/stopwords_least.txt",
    keywordPath: String = "",
    numIterations: Int = 20,
    simThreshold: Double = 0.66,
    groupLength: Int = 1000)

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("artifical question cluster") {
      opt[String]("input")
        .text("input path")
        .action((x, c) => c.copy(input = x))
      opt[String]("output")
        .text("output path")
        .action((x, c) => c.copy(output = x))
      opt[String]("stopFilePath")
        .text("stop file path")
        .action((x, c) => c.copy(stopFilePath = x))
      opt[String]("keywordPath")
        .text("key word file path")
        .action((x, c) => c.copy(keywordPath = x))
      opt[Int]("numIterations")
        .text("iterations number")
        .action((x, c) => c.copy(numIterations = x))
      opt[Double]("simThreshold")
        .text("sim threshold")
        .action((x, c) => c.copy(simThreshold = x))
      opt[Int]("groupLength")
        .text("group length")
        .action((x, c) => c.copy(groupLength = x))
    }
    parser.parse(args, defaultParams).map {
      params => run(params)
      //      params => test()
    }.getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params): Unit ={
    val conf = new SparkConf().setAppName("artifical question cluster")
    val sc = new SparkContext(conf)
    val stopFilePath = params.stopFilePath
    val numIterations = params.numIterations
    val inputPath = params.input
    val outputPath = params.output
    val simThreshold = params.simThreshold
    val keywordPath = params.keywordPath
    val groupLength = params.groupLength

    HDFSUtil.removeOrBackup(outputPath, outputPath)

    val tag = sc.textFile(keywordPath)
      .map(_.split("\t"))
      .map(_(0))
      .filter(_.length > 1)
      .collect()
      .toList

    val URLSTR = "((http|ftp|https)://)(([a-zA-Z0-9._-]+.[a-zA-Z]{2,6})|" +
      "([0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}))(:[0-9]{1,4})*(/[a-zA-Z0-9&%_./-~-]*)?"
    val data = sc.textFile(inputPath)
      .map(x => {
        val query = x.replaceAll(URLSTR, "")
        if(query.length > 200) query.substring(0, 199) else query
      })
      .cache()

    if(data.count() == 0){
      println("【数据异常，输入数据为空！】")
      System.exit(1)
    }

    val documents = data
      .distinct()
      .mapPartitions(iter => {
        Segment.init(true, stopFilePath, false, "")
        Segment.loadWords(tag)
        iter
      })
      .map(x => (x, FilterUtils.replaceSymbolToSpace(Segment.segment(FilterUtils.removeHeadAndTailSymbol(x), " "))
                      .split(" ")
                      .filter(y => FilterUtils.chineseRatioCheck(y, 1.0))
                      .toSeq))
      .cache()

    val questionCount = data
      .map(x => (x, 1))
      .reduceByKey(_+_)
      .cache()
    val clusterNum = questionCount.count().toInt / groupLength

    val tfCount = documents.flatMap(x => x._2).distinct().count()
    val hashingTF = new HashingTF(tfCount.toInt)
    val tf = hashingTF
      .transform(documents.map(_._2))
    val tfidf = new IDF()
      .fit(tf)
      .transform(tf)
      .cache()

    val clusters = KMeans.train(tfidf, clusterNum, numIterations, 1, KMeans.RANDOM)
    val bcc = sc.broadcast(clusters)
//    val clusterCenters = clusters.clusterCenters

    val tfIdfIndex = tfidf.zipWithIndex().map(x => (x._2, x._1))
    val docTfidf = documents.map(_._1)
      .zipWithIndex()
      .map(x => (x._2, x._1))
      .join(tfIdfIndex)
      .map(x => (x._2._2, x._2._1))

    //qc = (cluster_id, (query, query_count))
    val qc = tfidf.distinct()
      .map((Random.nextInt(1024), _))
      .partitionBy(new HashPartitioner(8))
      .map(_._2).map(v => (v, bcc.value.predict(v)))
      .join(docTfidf)
      .map(x => (x._2._1, x._2._2))
      .distinct()
      .cache()

    val clusterDP = qc.map(x => (x._1, List(x._2)))
      .reduceByKey(_++_)
      .map((Random.nextInt(8192), _))
      .partitionBy(new HashPartitioner(64))
      .map(_._2)
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
//        val u1 = (for(i <- 0 until x.length; j <- i + 1 until x.length) yield {
//          (x(i), x(j), CalSimilar.calEditSimilar(
//            FilterUtils.replaceSymbolToSpace(
//              FilterUtils.removeHeadAndTailSymbol(x(i))),
//            FilterUtils.replaceSymbolToSpace(
//              FilterUtils.removeHeadAndTailSymbol(x(j)))))
//        }).filter()
//        val u2 = x.map(x => (x, x, 1.0))
//        val u

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
            //方案3 句子在聚类中3个标识1为聚类内容 0为聚类边缘 如果相似问句在已有聚类的标识为1便加入该聚类并且标识0 否则作为新聚类
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
      .flatMap(x => x._2.map(y => (x._1 + "_" + y._1, y._2, y._3)))
      .cache()
//    val clusterC = clusterDP
//      .map(x => (x._1, 1))
//      .reduceByKey(_+_)
//      .sortBy(_._2, false)
//      .cache()
//    val cMaxW = (clusterC.take(1)(0)._2 - 1).toDouble
//    val clusterW = clusterC.map(x => (x._1, (x._2 - 1).toDouble/cMaxW))

    //output:cluster_id,flag,weight,query_count,query
//    clusterDP.map(x => (x._1, (x._2, x._3)))
//      .join(clusterW)
//      .map(x => (x._2._1._1, (x._1, x._2._1._1, x._2._1._2, x._2._2)))
//      .join(questionCount)
//      .map(x => List(x._2._1._1, x._2._1._3, x._2._1._4, x._2._2, x._2._1._2))
//      .sortBy(x => (x(2).toString.toDouble, x(0).toString, x(1).toString.toInt, x(3).toString.toInt), false)
//      .map(_.mkString("|"))
//      .saveAsTextFile(outputPath)

    // 每个聚类再进行细分
    // 对聚类中的问句进行分词，保留n开头的词性，并进行词频统计
    // 选出每个句子在聚类中词频最高的词，按词分组作为聚类细分结果
    val clusterGroup = clusterDP.map(x => (x._2, (x._1, x._2, x._3)))
      .join(questionCount)
      .map(x => (x._2._1._1, x._2._1._2, x._2._1._3, x._2._2))
      .map(x => (x, x._2))
      .mapPartitions(iter => {
        Segment.init(true, stopFilePath, false, "")
        Segment.loadWords(tag)
        iter
      }).map(x => {
        (x._1, Segment.nlpSegmentWithPos(x._2, " ", ":"))
      })
      .map(x => (x._1._1, List((x._1, x._2))))
      .reduceByKey(_ ++ _)
      .flatMap(x => {
        val detail = x._2
        val wc = detail.flatMap(y => y._2.split(" ").filter(z => z.contains(":n")))
          .map(y => (y, 1))
          .groupBy(_._1)
          .map(y => (y._1, y._2.length)).toList
          .sortWith((a, b) => a._2 > b._2)
          .zipWithIndex
          .map(y => (y._1._1, y._2))
          .toMap
        detail.map(y => {
          val wd = y._2.split(" ")
            .map(z => (z, if(wc.contains(z)) wc(z) else 0)).toList
            .sortWith((a, b) => a._2 > b._2)
            .take(1)(0)
          (y._1._1 + "_" + wd._2, y._1._3, 0.0, y._1._4, y._1._2)
        })
      })
      .cache
    // 将聚类个数归一化作为权重
    val clusterC = clusterGroup
      .map(x => (x._1, 1))
      .reduceByKey(_+_)
      .sortBy(_._2, false)
      .cache()
    val cMaxW = (clusterC.take(1)(0)._2 - 1).toDouble
    val clusterW = clusterC.map(x => (x._1, (x._2 - 1).toDouble/cMaxW))

    //output:cluster_id,flag,weight,query_count,query
//    clusterGroup.map(x => (x._1, x))
//      .join(clusterW)
//      .map(x => List(x._1, x._2._1._2, x._2._2, x._2._1._4, x._2._1._5))
//      .sortBy(x => (x(2).toString.toDouble, x(0).toString, x(1).toString.toInt, x(3).toString.toInt), false)
//      .map(_.mkString("|"))
//      .saveAsTextFile(outputPath)
    //claster_id 映射为单批次唯一值
    val result = clusterGroup.map(x => (x._1, x))
      .join(clusterW).map(x => (x._1, x._2._1._2, x._2._2, x._2._1._4, x._2._1._5))
    val distinct_cluster = result.map(x => x._1).distinct().map(x => (x, StrUtil.uuid))
    result.map(x => (x._1, x))
      .join(distinct_cluster)
      .map(x => List(x._2._2, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5))
      .sortBy(x => (x(2).toString.toDouble, x(0).toString, x(1).toString.toInt, x(3).toString.toInt), false)
      .map(_.mkString("|"))
      .saveAsTextFile(outputPath)

    questionCount.unpersist()
    tfidf.unpersist()
    documents.unpersist()
    qc.unpersist()
    clusterGroup.unpersist()
  }
}
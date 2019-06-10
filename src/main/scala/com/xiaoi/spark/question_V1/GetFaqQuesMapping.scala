package com.xiaoi.spark.question

import com.xiaoi.spark.util.UnansQuesUtil
import com.xiaoi.common.{HDFSUtil, InputPathUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

/**
 * 1.获取一个月数据的词数，标准问，回答标志。（用于knn）
 * 2.从日志中得到标准问和标准问ID的映射，如果有多个，选择时间最近的。
 * Created by ligz on 15/10/12.
 */
object GetFaqQuesMapping {

  /**
    * 获取一个月数据的词数，标准问，回答标志。（用于knn）
    * @param inputData
    * @param excludeFaqs
    */
  def askCNTandAnswered(inputData: RDD[String], excludeFaqs: Array[String], params: Params):Unit = {
    val minQuesLen = params.minQuesLen
    val maxQuesLen = params.maxQuesLen
    val similarity = params.similarity
    val illegalCharNum = params.illegalCharNum
    val recentlyQuestionsPath = params.recentlyQuestionsPath
    val outputPartNum = params.outputPartitionNum

    val ques_faq = inputData.map(x => {
      val fields = x.split("\\|", -1)
      val ori_ques = fields(3)
      val ques = UnansQuesUtil.quesSimplify(ori_ques)
      val ex = if (fields.length < 16) null else fields(15)
      val ans_type = fields(6).toInt
      val faq_name = fields(8)
      val sim = fields(12)
      (ques, faq_name, sim, ex, ori_ques, ans_type)
    }).filter { case (ques, faq_name, sim, ex, ori_ques, ans_type) =>
      UnansQuesUtil.normalCheck(ques, ex, minQuesLen, maxQuesLen, illegalCharNum, 4)
    }.filter(x => !excludeFaqs.contains(x._2))

    // 简洁问题、原问题、标准问、是否已回答
    // 一旦有可以能够回答的（相似度大于阈值），就认为是已经回答
    val ques_ori = ques_faq.map(x => {
      val faq = x._2
      val answered = if (x._3.toDouble > similarity && x._2.trim.length > 0) true else false
      ((x._1, x._5), (faq, answered))
    }).foldByKey(("", false))((acc, ele) => {
      if (ele._2)
        ele
      else if (!acc._2)
        ele
      else
        acc
    }).map(x => (x._1._1, (x._1._2, x._2._1, x._2._2)))

    val ques_cnt = ques_ori.map(x => (x._1, 1)).reduceByKey(_ + _)

    // 简洁问题、次数、原问题、标准问、已回答标志
    val ques_cnt_ori = ques_cnt.join(ques_ori.distinct())
      .map(x => (x._1, x._2._1, x._2._2._1, x._2._2._2, x._2._2._3))

    ques_cnt_ori.sortBy(_._2, false)
      .map(x => x._1 + "|" + x._2 + "|" + x._3 + "|" + x._4 + "|" + x._5)
      .repartition(outputPartNum).saveAsTextFile(recentlyQuestionsPath)
  }

  /**
    * 获取知识点对应ID，对于一个知识点有多个ID与之对应的情况，获取最新的一个知识点和ID映射
    * @param input_data
    * @param faqMappingPath
    */
  def faqMapping(input_data: RDD[String], faqMappingPath: String):Unit = {
    val faqIds = input_data.map(x => {
      val fields = x.split("\\|")
      (fields(8), (fields(7), fields(0)))
    }).filter(x => {x._1 != null && x._1.trim.length > 0})
      .map(x => (x._1, List[(String, String)](x._2)))
      .reduceByKey(_ ++ _)
      .mapValues(iter => {
        iter.sortWith((a, b) => a._2 > b._2).take(1)
      }).flatMapValues(x => x).map(x => (x._1, x._2._1))

    faqIds.map(x => x._1 + "|" + x._2).saveAsTextFile(faqMappingPath)
  }

  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName("GetFaqQuesMapping")
    val sc = new SparkContext(conf)

    val inputPath = params.inputPath
    val targetDate = params.targetDate
    val days = params.days
    val removeMode = params.removeMode
    val dfsUri = params.dfsUri
    val minQuesLen = params.minQuesLen
    val maxQuesLen = params.maxQuesLen
    val similarity = params.similarity
    val illegalCharNum = params.illegalCharNum
    val recentlyQuestionsPath = params.recentlyQuestionsPath
    val faqMappingPath = params.faqMappingPath
    val excludeFaqsPath = params.excludeFaqsPath
    val outputPartNum = params.outputPartitionNum

    HDFSUtil.removeOrBackup(removeMode,dfsUri,recentlyQuestionsPath)
    HDFSUtil.removeOrBackup(removeMode,dfsUri,faqMappingPath)

    // 排除标准问的文件路径
    val excludeFaqs = if (HDFSUtil.exists(dfsUri, excludeFaqsPath))
      sc.textFile(excludeFaqsPath).filter(_.trim.length > 0).collect()
    else Array[String]()

    // 处理一天数据或者最近n天数据
    val fromD = InputPathUtil.getTargetDate(targetDate)
    val inputData = sc.textFile(InputPathUtil.getInputPath(days, fromD.plusDays(1), inputPath)).cache()

    //获取一个月数据的词数，标准问，回答标志。
    askCNTandAnswered(inputData, excludeFaqs, params)

    //获取知识点对应ID，对于一个知识点有多个ID与之对应的情况，获取最新的一个知识点和ID映射
    faqMapping(inputData, faqMappingPath)

    inputData.unpersist()
    sc.stop()

  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("Get Questions in Faq for a period") {
      head("Get Questions in Faq for a period")
      opt[String]('i', "input")
        .text("Input path for ask data")
        .action((x, c) => c.copy(inputPath = x))
      opt[String]("targetDate")
        .text("target date(example: 2016/01/01),default 0 for yesterday")
        .action((x, c) => c.copy(targetDate = x))
      opt[String]("excludeFaqsPath")
        .text("需要排除的标准问文件路径")
        .action((x, c) => c.copy(excludeFaqsPath = x))
      opt[Int]('d', "days")
        .text("how many days to analysis")
        .action((x, c) => c.copy(days = x))
      opt[String]('b', "recentlyQues")
        .text("Path for recently Questions cnt")
        .action((x, c) => c.copy(recentlyQuestionsPath = x))
      opt[String]('m', "faqMappingPath")
        .text("Path for faq and faqID mapping path")
        .action((x, c) => c.copy(faqMappingPath = x))
      opt[Int]("outputPartitions")
        .text("输出文件分区数")
        .action((x, c) => c.copy(outputPartitionNum = x))
      opt[Int]('s', "minQuesLen")
        .text("min question length")
        .action((x, c) => c.copy(minQuesLen = x))
      opt[Int]('l', "maxQuesLen")
        .text("max question length")
        .action((x, c) => c.copy(maxQuesLen = x))
      opt[Int]('c', "illegalCharNum")
        .text("illegal char number")
        .action((x, c) => c.copy(illegalCharNum = x))
      opt[Double]('x', "similarity")
        .text("greater than this similarity value will be viewed well answered")
        .action((x, c) => c.copy(similarity = x))
      opt[String]('u', "dfsUri")
        .text("hadoop fs default name")
        .action((x, c) => c.copy(dfsUri = x))
      opt[Boolean]('r', "removeMode")
        .text("remove or backup output dir that exists")
        .action((x, c) => c.copy(removeMode = x))
      checkConfig { params => success }

    }

    parser.parse(args, defaultParams).map {
      params => run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  case class Params(
                     inputPath: String = "",
                     targetDate: String = "0",
                     excludeFaqsPath: String = "/production/nlp/config/exclude_faqs",
                     days: Int = 30,
                     minQuesLen: Int = 2,
                     maxQuesLen: Int = 50,
                     illegalCharNum: Int = 4,
                     similarity: Double = 0.73,
                     outputPartitionNum: Int = 64,
                     recentlyQuestionsPath: String = "",
                     faqMappingPath: String = "",
                     dfsUri: String = "hdfs://172.16.0.1:9000",
                     removeMode: Boolean = true
                     )

}

package com.xiaoi.spark.artificial_v1

import com.xiaoi.common.{FilterUtils, HDFSUtil}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import scopt.OptionParser

import scala.io.Source

/**
 * created by yang.bai@xiaoi.com
 * 人工客服QA抽取
 * 业务关键词获取->关键词匹配问句->疑问词库、否定负面词库、现象词库过滤问句
 * ->筛选问句附近的回答->疑问词库、客服礼貌、无法解决问题回答库过滤回答
 * ->聚类
 */
object GenerateQA {

  case class Params(
    input: String = "/experiment/baiy/lenovo_qa_45",
    qaOutput: String = "/experiment/baiy/output/lenovo_generate_qa_pro/qa",
    questionOutput: String = "/experiment/baiy/output/lenovo_generate_qa_pro/user_question",
    tagPath: String = "/experiment/baiy/lenovo_keys",
    qaHistoryPath: String = "/experiment/baiy/output/lenovo_generate_qa_pro/qa_history",
    interrogaWordsPath: String = "",
    privativeWordsPath: String = "",
    phenomenonWordsPath: String = "",
    customerServiceWordsPath: String = "",
    historyEnable: Boolean = false,
    questionChineseRatio: Double = 0.3,
    answerChineseRatio: Double = 0.7,
    answerNum: Int = 3,
    questionMinLength: Int = 4,
    answerMinLength: Int = 7,
    table: String = "qa_question",
    jdbcUrl: String = "jdbc:mysql://172.16.0.34:3306/lenovo_analysis",
    dbUser: String = "lenovo",
    dbPWD: String = "lenovo_Analysis1102")

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("lenovo QA抽取") {
      opt[String]("input")
        .text("input path")
        .action((x, c) => c.copy(input = x))
      opt[String]("questionOutput")
        .text("output path")
        .action((x, c) => c.copy(questionOutput = x))
      opt[String]("qaOutput")
        .text("output path")
        .action((x, c) => c.copy(qaOutput = x))
      opt[String]("tagPath")
        .text("tag path")
        .action((x, c) => c.copy(tagPath = x))
      opt[String]("qaHistoryPath")
        .text("qa history path")
        .action((x, c) => c.copy(qaHistoryPath = x))
      opt[String]("table")
        .text("table name")
        .action((x, c) => c.copy(table = x))
      opt[String]("jdbcUrl")
        .text("jdbc url")
        .action((x, c) => c.copy(jdbcUrl = x))
      opt[String]("dbUser")
        .text("database user")
        .action((x, c) => c.copy(dbUser = x))
      opt[String]("dbPWD")
        .text("database password")
        .action((x, c) => c.copy(dbPWD = x))
      opt[String]("interrogaPath")
        .text("interroga words path")
        .action((x, c) => c.copy(interrogaWordsPath = x))
      opt[String]("privativePath")
        .text("privative words path")
        .action((x, c) => c.copy(privativeWordsPath = x))
      opt[String]("phenomenonPath")
        .text("phenomenon words path")
        .action((x, c) => c.copy(phenomenonWordsPath = x))
      opt[String]("customerServiceWordsPath")
        .text("customer service words path")
        .action((x, c) => c.copy(customerServiceWordsPath = x))
      opt[Boolean]("historyEnable")
        .text("history enable")
        .action((x, c) => c.copy(historyEnable = x))
      opt[Double]("questionChineseRatio")
        .text("question chinese ratio")
        .action((x, c) => c.copy(questionChineseRatio = x))
      opt[Double]("answerChineseRatio")
        .text("answer chinese ratio")
        .action((x, c) => c.copy(answerChineseRatio = x))
      opt[Int]("answerNum")
        .text("answer number")
        .action((x, c) => c.copy(answerNum = x))
      opt[Int]("questionMinLength")
        .text("question min length")
        .action((x, c) => c.copy(questionMinLength = x))
      opt[Int]("answerMinLength")
        .text("answer min length")
        .action((x, c) => c.copy(answerMinLength = x))
    }
    parser.parse(args, defaultParams).map {
      params => run(params)
//      params => test()
    }.getOrElse {
      sys.exit(1)
    }
  }

  def removeAllPunctuates(input: String) : String = {
    val regex = "[^\\u4e00-\\u9fa5a-zA-Z0-9]";
    input.replaceAll(regex, "")
  }

  def run(params: Params): Unit ={
    val conf = new SparkConf().setAppName("generate qa")
    val sc = new SparkContext(conf)

    val input = params.input
    val qOutput = params.questionOutput
    val qaOutput = params.qaOutput
    val tagPath = params.tagPath
    val qaHistoryPath = params.qaHistoryPath
    val questionChineseRatio = params.questionChineseRatio
    val answerChineseRatio = params.answerChineseRatio
    val answerNum = params.answerNum
    val questionMinLength = params.questionMinLength
    val answerMinLength = params.answerMinLength
    val dateFormat = "yyyy/MM/dd HH:mm:ss"
    val dateFormatNew = "yyyy-MM-dd HH:mm:ss"
    val dateHistoryFormat = "yyyy_MM_dd_HH_mm_ss"
    val historyEnable = params.historyEnable

    if(HDFSUtil.exists(qaOutput, qaOutput)){
//      HDFSUtil.backupDir(qaOutput, qaHistoryPath + "/"
//        + DateTime.now().toString(dateHistoryFormat))
      HDFSUtil.backupDir(qaOutput, qaOutput, qaHistoryPath)
    }
    HDFSUtil.removeOrBackup(qOutput, qOutput)
    HDFSUtil.removeOrBackup(qaOutput, qaOutput)

    // 从MYSQL中取出已处理问句和未处理问句 用于和当前批次数据合并
    val sqlContext = new SQLContext(sc)
    val table = params.table
    val url = params.jdbcUrl
    val user = params.dbUser
    val pwd = params.dbPWD
    val history =
      sqlContext.read.format("jdbc").options(Map(
        "url" -> url,
        "dbtable" -> table,
        "user" -> user,
        "password" -> pwd
      )).load()
    val unhandleQues =
      if(historyEnable) history.filter("STATUS = 'unhandled'").rdd.map(_.getAs[String]("UNANS_QUES"))
      else sc.makeRDD(List[String]())
    val delQues =
      if(historyEnable) history.filter("STATUS != 'unhandled'").rdd.map(_.getAs[String]("UNANS_QUES"))
      else history.filter("STATUS = 'ignored'").rdd.map(_.getAs[String]("UNANS_QUES"))

    // 业务关键词
    val tag = sc.broadcast(
      sc.textFile(tagPath)
        .map(_.split("\t"))
        .map(_(0))
        .filter(_.length > 1)
        .collect()
        .toSet)

    val unhandleQuesRdd = unhandleQues.map(y=>(y, 1))
    // 历史QA数据在HDFS维护 用于和当前批次数据合并
    val qaHistory = if(historyEnable && HDFSUtil.exists(qaHistoryPath, qaHistoryPath + "/*")) {
      sc.textFile(qaHistoryPath + "/*/*")
      .map(x => {
        val lSplit = x.split("\\,", -1)
        (lSplit(0), lSplit(1))
      }).join(unhandleQuesRdd)
      .map(x=>(x._1, x._2._1))
    } else sc.makeRDD(List[Tuple2[String, String]]())

    // id,uid,nicename,marks,caseid,engid,engineer,
    // content,type,is_flag,send_time,create_time
    // is_flag:  200:客服说话 100:用户说话
    // type:     100:普通输入 200:超链接，图片 300:超链接，声音
    // uid+caseid 可以唯一确定一次session
    val data =sc.textFile(input)
      .map(_.split("\\,", -1))
      .cache()

    // 确保正常解析日期格式
    val formatDate = data
      .map(x => {
        x(3) = try{
          (DateTimeFormat.forPattern(dateFormatNew).parseDateTime(x(3))).toString(dateFormatNew)
        } catch {
          case ex: Exception => {
            println(x(3))
            ex.printStackTrace()
            ""
          }
        }
        x
      }).cache()
    val detail = formatDate.map(x => ((x(0), x(1)), List(x)))
//      .filter(x => !"".equals(x._2(0)(3)))
      .reduceByKey((a ,b) => a ++ b)
      .map(x => (x._1, x._2.sortBy(_(3)).zipWithIndex))
      .cache()
    val URLSTR = "((http|ftp|https)://)(([a-zA-Z0-9._-]+.[a-zA-Z]{2,6})|" +
      "([0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}))(:[0-9]{1,4})*(/[a-zA-Z0-9&%_./-~-]*)?"
    def localFilterWords(localPath: String): List[String] = {
      val localFile = Source.fromFile(localPath)
      val filterWords = localFile.getLines().toList
      localFile.close()
      filterWords
    }
    val interrogaWords =
      if("".equals(params.interrogaWordsPath)) List[String]()
      else localFilterWords(params.interrogaWordsPath)
    val privativeWords =
      if("".equals(params.privativeWordsPath)) List[String]()
      else localFilterWords(params.privativeWordsPath)
    val phenomenonWords =
      if("".equals(params.phenomenonWordsPath)) List[String]()
      else localFilterWords(params.phenomenonWordsPath)
    val filterWords = interrogaWords.union(privativeWords).union(phenomenonWords)
//    val D_WORDS = List("很","太","失","特别","非常","经常","依旧","总是","一直","有时","无法","无反应","慢","卡","坏","裂","碎")
//    val uq = formatDate.map(x => ((x(1), x(4)), List(x)))
//      .filter(x => !"".equals(x._2(0)(10)) && !"".equals(x._2(0)(11)))
//      .reduceByKey((a ,b) => a ++ b)
//      .flatMap(x => {
//        x._2.sortBy(y => y(10)).filter(y => "100".equals(y(9))).take(10)
//      })
    val delQuesRdd = delQues.map(x => (x, 1))
    val uq = detail
      .flatMap(x => {
        x._2.filter(y => "100".equals(y._1(2)))
//          .filter(y => y._2 <= 10)
       })
      .map(x => (x, x._1(4).replaceAll(URLSTR, "")))
      .filter(x => FilterUtils.chineseRatioCheck(x._2, questionChineseRatio))
      .filter(x => tag.value.exists(y => x._2.indexOf(y) != -1 && !x._2.equals(y)))
      .map(x => ((x._1._1(0), x._1._1(1)), (x._1._1(4), x._1._2)))
      .filter(x => {
        x._2._1.length >= questionMinLength &&
          (filterWords.length < 1 || filterWords.exists(y => x._2._1.contains(y)))
      })
      .cache()
    uq.map(x => (x._2._1, 1))
      .leftOuterJoin(delQuesRdd)
      .filter(_._2._2 == None)
      .map(_._1)
      .union(unhandleQues)
      .saveAsTextFile(qOutput)
//    uq.map(_._2._1).saveAsTextFile(qOutput)
//    val ca = formatDate.map(x => ((x(1), x(4)), List(x)))
//      .filter(x => !"".equals(x._2(0)(10)) && !"".equals(x._2(0)(11)))
//      .reduceByKey((a ,b) => a ++ b)
//      .flatMap(x => {
//        x._2.sortBy(y => y(10)).filter(y => "200".equals(y(9))).take(10)
//      })
    val customerServiceWords =
     if("".equals(params.customerServiceWordsPath)) List[String]()
     else localFilterWords(params.customerServiceWordsPath)
    // 整合疑问词和客服礼貌用语、和无用回答
    val filterCustomerAnswerWords = interrogaWords.union(customerServiceWords)
    val ca = detail
      .flatMap(x => {
        x._2.filter(y => "200".equals(y._1(2)))
//          .filter(y => y._2 <= 15)
      })
      .map(x => ((x._1(0), x._1(1)), (x._1(4), x._2)))
//      .filter(x => x._2._1.replaceAll(" ", "").length >= answerMinLength &&
//        !x._2._1.trim.endsWith("?") &&
//        !x._2._1.trim.endsWith("吗") &&
//        !x._2._1.trim.endsWith("？") &&
//        !x._2._1.trim.endsWith("呢") &&
//        !x._2._1.trim.endsWith("么") &&
//        !filterCustomerAnswerWords.exists(y => x._2._1.contains(y)) &&
//        FilterUtils.chineseRatioCheck(x._2._1.trim, answerChineseRatio))
      .cache()
    uq.distinct().join(ca.distinct())
      .map(x => ((x._1._1, x._1._2, x._2._1._1), List((x._2._2._1, x._2._2._2 - x._2._1._2))))
      .filter(_._2(0)._2 > 0)
      .reduceByKey(_ ++ _)
      .flatMap(x => {
        val qa = x._2.sortBy(_._2)
        if(qa.length > 0)
          qa.take(answerNum).map(y => ( x._1._1, x._1._2, x._1._3, y._1))
//            (x._1._1, x._1._2, x._1._3, qa.take(3)(0)._1)
        else List((x._1._1, x._1._2, x._1._3, ""))
       })
      .filter(x => !"".equals(x._4))
      .filter(x => x._4.replaceAll(" ", "").length >= answerMinLength &&
        !x._4.trim.endsWith("?") &&
        !x._4.trim.endsWith("吗") &&
        !x._4.trim.endsWith("？") &&
        !x._4.trim.endsWith("呢") &&
        !x._4.trim.endsWith("么") &&
        !filterCustomerAnswerWords.exists(y => x._4.contains(y)) &&
        FilterUtils.chineseRatioCheck(x._4.trim, answerChineseRatio))
      .map(x => {
        val query = x._3.replaceAll(URLSTR, "")
        (if(query.length > 200) query.substring(0, 199) else query, x._4)
      })
      .leftOuterJoin(delQuesRdd)
      .filter(_._2._2 == None)
      .map(x => (x._1, x._2._1))
      .union(qaHistory)
      .distinct()
      .map(x => x._1 + "|" + x._2)
      .saveAsTextFile(qaOutput)
  }
}

package com.xiaoi.spark.question

import com.xiaoi.conf.ConfigurationManager
import com.xiaoi.constant.Constants
import com.xiaoi.spark.util.UnansQuesUtil
import com.xiaoi.common.{HDFSUtil, InputPathUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

import scala.io.Source

/**
  * 合并GetUnanswered CountUnansWeek CountUnansMonth MergeUnansCnt
  * Created by ligz on 15/10/9.
  */
object GetUnanswered {

  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName("CountUnansweredCnt")
    val sc = new SparkContext(conf)

    val days_list = params.analyzeDays.replaceAll("\\s+", "").split(",").toList.map(_.toInt)
    val day = days_list(0)
    val week = days_list(1)
    val month = days_list(2)

    //获取指30天内的指定回答类型(0 11)的问题列表  GetRecentUnans
    //RDD: unans, count
    getRecentUnans(sc, params)

    //Filter data and get yesterday unanswered questions
    val unanswer = getUnanwered(sc, params, day)
    unanswer.cache()

    //最近1天未回答问题输出
    unanswerOutput(unanswer, params)

    //获取指定周期内的answer-type为0和11的问题的相应的被问次数
    val countByDay = unanswerCountDay(sc, params, unanswer)
      countByDay.map(x => x._1 + "|" + x._2 + "|" + x._3).repartition(8).saveAsTextFile(params.dayCntPath)
    val countByWeek = unanswerCount(sc, params, week)
      countByWeek.map(x => x._1 + "|" + x._2 + "|" + x._3).saveAsTextFile(params.weekCntPath)
    val countByMonth = unanswerCount(sc, params, month)
      countByMonth.map(x => x._1 + "|" + x._2 + "|" + x._3).saveAsTextFile(params.monthCntPath)

    //简化问题-原始问题及未回答和建议问频次信息输出
    questionMap(unanswer, countByDay, params)

    /**
      * 合并未回答问题的不同周期（日、周、月）、
      * 不同类型（answer-type为：0-未回答 或 11-建议问）的次数
      * 以及相似问中未回答问题的次数
      */
//    mergeUnansCnt(countByDay, countByWeek, countByMonth)   //缺少相关问count,暂不执行


    sc.stop()

  }

  /**
    * 获取指30天内的指定回答类型(0 11)的问题列表
    * @param sc
    * @param params
    */
  def getRecentUnans(sc: SparkContext, params: Params): Unit = {
    val fromD = InputPathUtil.getTargetDate("0")
    val input_data = sc.textFile(InputPathUtil.getInputPath(params.days, fromD.plusDays(1), params.inputPath)).cache()

    //0, 11
    val ansTypes = params.ansTypes.replaceAll("\\s+", "").split(",").toList

    // 不能认作回答良好的答案(包含:无法解答)的文件路径
    val bc_unclear = if (HDFSUtil.exists(params.dfsUri, params.unclearAnswerPath)) {
      val unclear = sc.textFile(params.unclearAnswerPath).filter(_.trim.length > 0).collect()
      sc.broadcast(unclear)
    } else null

    val bc_ignored = if (HDFSUtil.exists(params.dfsUri, params.ignoredQuesPath)) {
      val ignored_ques = sc.textFile(params.ignoredQuesPath)
        .map(UnansQuesUtil.ignoreQuesSimplify(_))
        .filter(_.length > 0).distinct.collect()
      sc.broadcast(ignored_ques)
    } else null

    // 业务关键词
    val domainWords = Source.fromFile(params.domainWordsPath).getLines().toList.map(_.toLowerCase())
    val bc_domainWords = if (domainWords.size > 0) sc.broadcast(domainWords) else null

    val unans_type = input_data.map(x => {
      val fields = x.split("\\|")
      val ori_ques = fields(3)
      val ans = fields(5)
      val ques = UnansQuesUtil.quesSimplify(ori_ques)
      val ex = if (fields.length < 16) null else fields(15)
      val ans_type = if (bc_unclear != null &&
        bc_unclear.value.exists(p => ans.contains(p))) "0"
      else fields(6)
      (ques, ans_type, ex)
    }).filter({ case (ques, ans_type, ex) => ansTypes.contains(ans_type) })
      .filter({ case (ques, ans_type, ex) =>
        UnansQuesUtil.quesCheck(ques, ex) && (ques != null) && (ques.length > 1)
      }).filter({ case (ques, ans_type, ex) =>
      if (bc_ignored != null) {
        val short_q = UnansQuesUtil.ignoreQuesSimplify(ques)
        !bc_ignored.value.contains(short_q)
      } else true
    }).filter {// 过滤得到包含业务词但不仅仅包含业务词的问句
      case (ques, ans_type, ex) => {
        val lower_ques = ques.toLowerCase()
        if (bc_domainWords != null){
          bc_domainWords.value.exists(p => lower_ques.contains(p) && lower_ques.trim.length > p.length) &&
            !bc_domainWords.value.exists(p => lower_ques.equals(p))
        }else{
          true
        }
      }
    }.filter(_._1.length>2) //问句长度大于2
      .distinct().map(x => x._1 -> x._2)
      .reduceByKey((a, b) => if (a == "0" || b == "0") "0" else a) // 同一个问题,既有未回答也有建议问的情况,取未回答
    unans_type.map(x => x._1 + "|" + x._2).saveAsTextFile(params.recent_unans)
  }

  /**
    * 简化问题-原始问题及未回答和建议问频次信息输出
    * @param unans
    * @param unans_cnt
    * @param params
    */
  def questionMap(unans: RDD[(String, String, String)],
                  unans_cnt: RDD[(String, Int, Int)],
                  params: Params): Unit = {
    // 未回答问题、原始问题
    val unans_ori = unans.map(x => (x._1, x._3)).distinct()
    // 未回答问题、原始问题、answer-type=0次数、answer-type=11次数
    val unans_cnt_ori = unans_cnt.map(x => (x._1, (x._2, x._3))).join(unans_ori)
      .map(x => (x._1, x._2._2, x._2._1._1, x._2._1._2)).sortBy(_._3, false)
    unans_cnt_ori.map(x => x._1 + "|" + x._2 + "|" + x._3 + "|" + x._4)
      .repartition(8).saveAsTextFile(params.quesMapPath)
  }

  /**
    * 最近n天未回答问题输出
    * @param unans
    * @param params
    */
  def unanswerOutput(unans: RDD[(String, String, String)], params: Params): Unit = {
    unans.map(x => (x._1, 1)).reduceByKey(_ + _)
      .sortBy(_._2, false)
      .map(x => x._1 + "|" + x._2)
      .repartition(4)
      .saveAsTextFile(params.oneDayUnanswer)
  }

  /**
    * Yesterday unanswer count
    * @param sc
    * @param params
    * @param unans
    * @return
    */
  def unanswerCountDay(sc: SparkContext, params: Params, unans: RDD[(String, String, String)]): RDD[(String, Int, Int)] = {
    val unans_cnt0 = unans.filter(_._2.toInt == 0).map(x => (x._1, 1)).reduceByKey(_ + _)
    val unans_cnt11 = unans.filter(_._2.toInt == 11).map(x => (x._1, 1)).reduceByKey(_ + _)
    // 未回答问题、answer-type=0次数、answer-type=11建议问次数
    val unans_cnt = unans.map(x => (x._1, x._1)).distinct()
      .leftOuterJoin(unans_cnt0)
      .leftOuterJoin(unans_cnt11)
      .map({ case (ques, ((q1, cnt0), cnt11)) => (ques, cnt0.getOrElse(0), cnt11.getOrElse(0)) })
      .cache()
    unans_cnt
  }

  /**
    * Unanswer question count
    * @param sc
    * @param params
    * @param day
    * @return
    */
  def getUnanwered(sc: SparkContext, params: Params, day: Int): RDD[(String, String, String)] ={
    val ansTypes = params.ansTypes.replaceAll("\\s+", "").split(",").toList

    // 处理一天数据或者最近n天数据
    val fromD = InputPathUtil.getTargetDate(params.targetDate)
    val input_data = sc.textFile(InputPathUtil.getInputPath(day, fromD.plusDays(1), params.inputPath))
      .cache()

    // 被认作是未回答的标准问(比如:电脑反问,手机反问,用户描述不清楚)的文件路径
    val bc_excludeFaqs = if (HDFSUtil.exists(params.dfsUri, params.excludeFaqsPath)) {
      val excludeFaqs = sc.textFile(params.excludeFaqsPath).filter(_.trim.length > 0).collect()
      sc.broadcast(excludeFaqs)
    } else null

    // 业务关键词
    val domainWords = Source.fromFile(params.domainWordsPath).getLines().toList.map(_.toLowerCase().replace(" ",""))
    val bc_domainWords = if (domainWords.size > 0) sc.broadcast(domainWords) else null

    // 不能认作回答良好的答案(包含:无法解答)的文件路径
    val bc_unclear = if (HDFSUtil.exists(params.dfsUri, params.unclearAnswerPath)) {
      val unclear = sc.textFile(params.unclearAnswerPath).filter(_.trim.length > 0).collect()
      sc.broadcast(unclear)
    } else null

    val bc_ignored = if (HDFSUtil.exists(params.dfsUri, params.ignoredQuesPath)) {
      val ignored_ques = sc.textFile(params.ignoredQuesPath)
        .map(UnansQuesUtil.ignoreQuesSimplify(_))
        .filter(_.length > 0).distinct.collect()
      sc.broadcast(ignored_ques)
    } else null

    // 得到昨日未回答问题, 过滤掉已处理和已忽略的问题
    val unans = input_data.map(x => {
      val fields = x.split("\\|")
      val ori_ques = fields(3).trim
      val ques = UnansQuesUtil.quesSimplify(ori_ques)
      val ans = fields(5)
      val faq = fields(8)
      val ex = if (fields.length < 16) null else fields(15)
      val ans_type = if (bc_excludeFaqs != null && bc_excludeFaqs.value.contains(faq)) "0"
      else if (bc_unclear != null && bc_unclear.value.exists(p => ans.contains(p))) "0"
      else fields(6)
      (ques, ans_type, ex, ori_ques)
    })
      .filter { case (ques, ans_type, ex, ori_ques) =>
        ansTypes.contains(ans_type)
      }
      .filter { case (ques, ans_type, ex, ori_ques) =>
        UnansQuesUtil.quesCheck(ques, ex, params.minQuesLen, params.maxQuesLen, params.illegalCharNum, params.dupCharNum)
      }
      .filter { case (ques, ans_type, ex, ori_ques) =>
        val short_ori = UnansQuesUtil.ignoreQuesSimplify(ori_ques)
        if (bc_ignored != null) !bc_ignored.value.contains(short_ori) else true
      } // 过滤已经忽略和处理过的问题
      .filter {
      case (ques, ans_type, ex, ori_ques) => {
        val lower_ques = ques.toLowerCase()
        if (bc_domainWords != null){
          bc_domainWords.value.exists(p => lower_ques.contains(p) && lower_ques.trim.length > p.length) &&
            !bc_domainWords.value.exists(p => lower_ques.equals(p))
        }else{
          true
        }
      }
    } // 过滤得到包含业务词但不仅仅包含业务词的问句
      .filter(_._1.length>2) //问句长度大于2
      .map({ case (ques, ans_type, ex, ori_ques) => (ques, ans_type, ori_ques) })
    unans
  }

  /**
    * 获取指定周期内的answer-type为0和11的问题的相应的被问次数
    * @param sc
    * @param params
    * @param analyzeDays
    * @return
    */
  def unanswerCount(sc: SparkContext, params: Params, analyzeDays: Int): RDD[(String, Int, Int)] = {
    val ansTypes = params.ansTypes.replaceAll("\\s+", "").split(",").map(_.toInt).toList

    // 被认作是未回答的标准问(比如:电脑反问,手机反问,用户描述不清楚)的文件路径
    val bc_excludeFaqs = if (HDFSUtil.exists(params.dfsUri, params.excludeFaqsPath)) {
      val excludeFaqs = sc.textFile(params.excludeFaqsPath).filter(_.trim.length > 0).collect()
      sc.broadcast(excludeFaqs)
    } else null

    // 不能认作回答良好的答案(包含:无法解答)的文件路径
    val bc_unclear = if (HDFSUtil.exists(params.dfsUri, params.unclearAnswerPath)) {
      val unclear = sc.textFile(params.unclearAnswerPath).filter(_.trim.length > 0).collect()
      sc.broadcast(unclear)
    } else null

    // 处理一天数据或者最近n天数据
    val fromD = InputPathUtil.getTargetDate(params.targetDate)
    val input_data = sc.textFile(InputPathUtil.getInputPath(analyzeDays, fromD.plusDays(1), params.inputPath)).cache()
    val unans = input_data.map(x => {
      val fields = x.split("\\|",-1)
      val ori_ques = fields(ConfigurationManager.getInt(Constants.FIELD_QUESTION))
      val ques = UnansQuesUtil.quesSimplify(ori_ques)
      val ans = fields(ConfigurationManager.getInt(Constants.FIELD_ANSWER))
      val faq = fields(ConfigurationManager.getInt(Constants.FIELD_FAQ_NAME))
      val ans_type = if (bc_excludeFaqs != null && bc_excludeFaqs.value.contains(faq)) "0"
      else if (bc_unclear != null && bc_unclear.value.exists(p => ans.contains(p))) "0"
      else fields(ConfigurationManager.getInt(Constants.FIELD_ANSWER_TYPE))
      val ex = if (fields.length < 16) null else fields(ConfigurationManager.getInt(Constants.FIELD_EX))
      (ques, ans_type.toInt, ex)
    }).filter({ case (ques, ans_type, ex) => ansTypes.contains(ans_type) })
      .filter({ case (ques, ans_type, ex) =>
        UnansQuesUtil.quesCheck(ques, ex, params.minQuesLen, params.maxQuesLen, params.illegalCharNum, params.dupCharNum)
      }).cache()

    type UnansCnt = (Int, Int)
    def addOp(a: UnansCnt, b: UnansCnt): UnansCnt = {
      (a._1 + b._1, a._2 + b._2)
    }

    // 获取未回答问题和给出建议问的问题的频次
    val unans_cnt1 = unans.map({ case (ques, ansType, ex) =>
      if (ansType == 0) (ques, (1, 0))
      else if (ansType == 11) (ques, (0, 1))
      else (ques, (0, 0))
    }).aggregateByKey((0, 0))(addOp, addOp)
      .map({ case (ques, (cnt0, cnt11)) => (ques, cnt0, cnt11) })

    unans_cnt1
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("Count Unanswered questions count in specified period") {
      head("Count Unanswered questions Frequency")
      opt[String]('i', "input")
        .text("Input path for ask data")
        .action((x, c) => c.copy(inputPath = x))
      opt[String]("ignoredQuesPath")
        .text("已忽略和已处理的问题路径")
        .action((x, c) => c.copy(ignoredQuesPath = x))
      opt[String]("excludeFaqsPath")
        .text("需要排除的标准问文件路径")
        .action((x, c) => c.copy(excludeFaqsPath = x))
      opt[String]("unclearAnswerPath")
        .text("不能认作回答良好的答案")
        .action((x, c) => c.copy(unclearAnswerPath = x))
      opt[String]("domainWordsPath")
        .text("领域(业务)关键词路径")
        .action((x, c) => c.copy(domainWordsPath = x))
      opt[String]("targetDate")
        .text("target date(example: 2016/01/01),default 0 for yesterday")
        .action((x, c) => c.copy(targetDate = x))
      opt[String]('d', "analyzeDays")
        .text("how many days to analysis")
        .action((x, c) => c.copy(analyzeDays = x))
      opt[Int]('s', "minQuesLen")
        .text("最短问题长度")
        .action((x, c) => c.copy(minQuesLen = x))
      opt[Int]('l', "maxQuesLen")
        .text("最长问题长度")
        .action((x, c) => c.copy(maxQuesLen = x))
      opt[Int]('c', "illegalCharNum")
        .text("非法字符个数")
        .action((x, c) => c.copy(illegalCharNum = x))
      opt[Int]('p', "dupCharNum")
        .text("重复字符个数")
        .action((x, c) => c.copy(dupCharNum = x))
      opt[String]("dfsUri")
        .text("hadoop fs default name")
        .action((x, c) => c.copy(dfsUri = x))
      opt[Boolean]('r', "removeMode")
        .text("remove or backup output dir that exists")
        .action((x, c) => c.copy(removeMode = x))
      opt[String]("ansTypes")
        .text("需要处理的回答类型,默认(0,11)为未回答和建议问")
        .action((x, c) => c.copy(ansTypes = x))
      opt[String]("recent_unans")
        .text("30 days unans and count")
        .action((x, c) => c.copy(recent_unans = x))
      opt[String]("oneDayUnanswer")
        .text("Output path for unanswered questions frequency")
        .action((x, c) => c.copy(oneDayUnanswer = x))
      opt[String]("unans_mapping")
        .text("简化问题-原始问题及未回答和建议问频次信息输出路径")
        .action((x, c) => c.copy(quesMapPath = x))
      opt[String]("dayCntPath")
        .text("Output path for unanswered questions frequency")
        .action((x, c) => c.copy(dayCntPath = x))
      opt[String]("weekCntPath")
        .text("简化问题-原始问题及未回答和建议问频次信息输出路径")
        .action((x, c) => c.copy(weekCntPath = x))
      opt[String]("monthCntPath")
        .text("Output path for unanswered questions frequency")
        .action((x, c) => c.copy(monthCntPath = x))
      opt[Int]("days")
        .text("analysis days")
        .action((x, c) => c.copy(days = x))
      checkConfig { params => success }

    }

    parser.parse(args, defaultParams).map {params =>
      HDFSUtil.removeOrBackup(params.dfsUri, params.recent_unans)
      HDFSUtil.removeOrBackup(params.removeMode, params.dfsUri, params.oneDayUnanswer)
      HDFSUtil.removeOrBackup(params.removeMode, params.dfsUri, params.quesMapPath)
      HDFSUtil.removeOrBackup(params.removeMode, params.dfsUri, params.dayCntPath)
      HDFSUtil.removeOrBackup(params.removeMode, params.dfsUri, params.weekCntPath)
      HDFSUtil.removeOrBackup(params.removeMode, params.dfsUri, params.monthCntPath)

      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  case class Params(
                     inputPath: String = "/production/lenovo/data/ask",
                     targetDate: String = "0",
                     analyzeDays : String = "1,7,30",
                     ignoredQuesPath: String = "hdfs://172.16.0.1:9000/production/nlp/output/ignored_ques",
                     excludeFaqsPath: String = "/production/nlp/config/exclude_faqs",
                     unclearAnswerPath: String = "/production/nlp/config/unclear_answer",
                     domainWordsPath: String = "",
                     ansTypes: String = "0, 11",
                     minQuesLen: Int = 2,
                     maxQuesLen: Int = 40,
                     illegalCharNum: Int = 4,
                     dupCharNum: Int = 4,
                     recent_unans: String = "/production/lenovo/output/unanswered_analysis/step_recent_unans",
                     oneDayUnanswer: String = "hdfs://172.16.0.1:9000/production/nlp/output/unanswered_analysis/step_filter",
                     quesMapPath: String = "hdfs://172.16.0.1:9000/production/nlp/output/unanswered_analysis/step_yesterday_unans_ori",
                     dayCntPath: String = "hdfs://172.16.0.1:9000/production/nlp/output/unanswered_analysis/step_cnt/oneday",
                     weekCntPath: String = "/production/lenovo/output/unanswered_analysis/step_cnt/sevendays",
                     monthCntPath: String = "/production/lenovo/output/unanswered_analysis/step_cnt/sevendays",
                     dfsUri: String = "hdfs://172.16.0.1:9000",
                     removeMode: Boolean = true,
                     days: Int = 30
                   )

}

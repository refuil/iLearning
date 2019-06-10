package com.xiaoi.spark.question

import com.xiaoi.spark.util.UnansQuesUtil
import com.xiaoi.common.{HadoopOpsUtil, InputPathUtil, Segment}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Days}
import scopt.OptionParser

/**
 * 计算转人工次数、建议问频次、未回答问句业务词权重和平台信息，为下一步进行摘要提取
 * created by yang.bai modify by josh
 */
object SemanticInfo {
  def  run(params: Params) {
    val conf = new SparkConf().setAppName("SemanticInfo")
    val sc = new SparkContext(conf)

    //得到当天未回答问题的平台信息，以及一段周期内对应问题的平台回答情况
    //RDD: 问题、昨日未回答平台、周期内已回答平台、周期内未回答平台
    getUnansPlatformInfo(sc, params)

    //对当天的建议问点击情况进行汇总
    //RDD: 问题，点击率，点击次数、未点击次数
    suggestSplit(sc, params)

    //计算句子是否和业务有关的权重
    //RDD: 问句，权重
    quesWeightByKeyword(sc, params)

    //计算指定周期内的转人工次数
    val days_list = params.analyzeDays.split(",").map(_.toInt)
    val day = days_list(0)
    val week = days_list(1)
    val month = days_list(2)
    //RDD: Manual_ques, count
    val countByDay = manualCount(sc, params, day)
    val countByWeek = manualCount(sc, params, week)
    val countByMonth = manualCount(sc, params, month)
    //RDD: Manual_ques, count
    val oneday_data = mergeManualCount(sc, countByDay)
    val week_data = mergeManualCount(sc, countByWeek)
    val month_data = mergeManualCount(sc, countByMonth)

    //合并1天 7天 1月的转人工次数
    mergeManualAll(oneday_data, week_data, month_data, params)

    //根据问题次数、触发转人工距离、问题时间计算问题转人工的置信度、等级
    toManualWeight(sc, params)

    sc.stop()
  }

  def quesWeightByKeyword(sc: SparkContext, params: Params): Unit = {
    //对昨日未回答问句分词，得到（分词，问句）
    val question = sc.textFile(params.oneDayUnanswer).map(x=>{
      val fields = x.split("\\|")
      if(fields != null && fields.length > 1){
        fields(0)
      }else{
        ""
      }
    }).filter(_.length>0).mapPartitions(x=>{
      Segment.init(true, params.stopWordsPath, false, "")
      x
    }).flatMap(x=>{
      val segs = Segment.segment(x," ")
      if(segs != null){
        segs.split(" ").map(y=>((y,x)))
      }else{
        Seq(("",""))
      }
    }).filter(_._1.length > 0).cache()

    //取业务关键词，（关键词，词频），此处是否改为熵？
    val domainWords = sc.textFile(params.domainWordsPath).map(x=>{
      val fields = x.split("\t")
      if(fields != null && fields.length > 1)(
        (fields(0), fields(1))
        )else{
        ("", "")
      }
    }).filter(_._1.length > 0).cache()

    //计算权重（问句，权重）
    question.leftOuterJoin(domainWords).map(x=>{
      val v1 = if(None.equals(x._2._2)) "0" else x._2._2.get
      val v2 = if(None.equals(x._2._2)) "0" else "1"
      (x._2._1, (v1.toDouble, v2.toDouble))
    }).reduceByKey((x, y)=>(x._1 + y._1, x._2 + y._2)).map(x=>{
      val weight = if(x._2._2 == 0.0) 0.0 else x._2._1/x._2._2
      getLevel(x._1, weight)
    }).sortBy(-_._2).map(x=>x._1+"|"+x._2)
      .saveAsTextFile(params.chat_list)

  }

  //计算权重
  def getLevel(ques: String, weight: Double):Tuple2[String, Int] = {
    var level = 2
    if(weight <= 0.5){
      level -= 1
    }
    if(weight <= 0.2){
      level -= 2
    }
    if(-1 == level && ques.length >= 7){
      level = 0
    }
    if(ques.length == 1){
      level = -1
    }
    (ques, level)
  }

  /**
    * 对当天的建议问点击情况进行汇总
    * @param sc
    * @param params
    */
  def suggestSplit(sc: SparkContext, params: Params): Unit ={
    // 不算做建议问被点击的问题，例如：转人工服务、查看提问技巧
    val excludeSuggests = sc.textFile(params.excludeSuggestPath)
      .filter(_.trim.length > 0).collect()

    // 处理一天数据或者最近n天数据
    val fromD = InputPathUtil.getTargetDate("0")
    val input_data = sc.textFile(InputPathUtil.getInputPath(params.days, fromD.plusDays(1), params.inputPath))
      .cache()

    // 最近一段时间的建议问列表
    val suggest_ques = input_data.map(_.split("\\|"))
      .map(x => {
        val ques = UnansQuesUtil.quesSimplify(x(3))
        (x(0), x(1), ques, x(6).toInt, x(5))
      }).filter(x => x._4 == 11).cache()


    // 昨日建议问的问题列表
    val yesterday_input = params.inputPath + "/" + fromD.toString("yyyy/MM/dd")
    val yesterday_suggest = sc.textFile(yesterday_input).map(_.split("\\|"))
      .map(x => {
        val ques = UnansQuesUtil.quesSimplify(x(3))
        (ques, x(6).toInt)
      }).filter(x => x._2 == 11).map(_._1).collect().toList
    val bc_yesterday_suggest = sc.broadcast(yesterday_suggest)

    println(s"SUGGEST CNT: ${suggest_ques.count()}")

    // 得到建议问的会话详情，根据时间排序
    val suggest_sessions = input_data.map(_.split("\\|"))
      .map(x => (x(0), x(1), x(3), x(6).toInt))
      .filter(x => x._4 == 11)
      .map(x => (x._2, List[(String, String)]((x._1, x._3))))
      .repartition(16)
      .reduceByKey(_ ++ _)
      .mapValues(iter => {
        iter.sortBy(_._1)
      })

    println(s"SUGGEST SESSIONS CNT: ${suggest_sessions.count()}")

    // 建议问是否被点击
    val suggest_split = suggest_ques.map(x => (x._2, (x._1, x._3, x._5)))
      .join(suggest_sessions)
      .map({case(sid, ((dt, ques, answer), sessions)) => {
        val nextRec = sessions.filter(_._1 > dt).take(1)
        val next_ques = if (nextRec.size == 1) nextRec(0)._2 else null
        val hit = if (next_ques != null && answer.indexOf(next_ques) >= 0
          && !excludeSuggests.contains(next_ques)) true else false
        //          && !List("查看提问技巧", "转人工服务").contains(next_ques)) true else false
        (sid, ques, hit, answer, next_ques)
      }})

    val zeroValue = (0, 0)

    val seqOp = (c:(Int, Int), h:Boolean) => {
      if (h) (c._1 + 1, c._2)
      else (c._1, c._2 + 1)
    }
    val combOp = (c1:(Int, Int), c2:(Int, Int)) => {
      (c1._1 + c2._1, c1._2 + c2._2)
    }

    // 问题，点击率，点击次数、未点击次数
    suggest_split.map(x => (x._2, x._3)).aggregateByKey(zeroValue)(seqOp, combOp)
      .map(x => (x._1, x._2._1.toDouble / (x._2._1 + x._2._2), x._2._1, x._2._2))
      .filter(x => bc_yesterday_suggest.value.contains(x._1))
      .map(x => List(x._1, x._2, x._3, x._4).mkString("|"))
      .saveAsTextFile(params.suggest_hit)
  }

  /**
    * 得到当天未回答问题的平台信息，以及一段周期内对应问题的平台回答情况
    * @param sc
    * @param params
    */
  def getUnansPlatformInfo(sc: SparkContext, params: Params): Unit = {
    // 处理一天数据或者最近n天数据
    val fromD = InputPathUtil.getTargetDate("0")
    val input_data = sc.textFile(InputPathUtil.getInputPath(params.days, fromD.plusDays(1), params.inputPath)).cache()
    val yesterday_input = params.inputPath + "/" + fromD.toString("yyyy/MM/dd")

    val bc_unclear_q = if (HadoopOpsUtil.exists(params.dfsUri, params.unclearAnswerPath)) {
      val unclear_q = sc.textFile(params.unclearAnswerPath).filter(_.trim.length > 0).collect().toList
      sc.broadcast(unclear_q)
    } else null

    //昨日未回答和对应平台（ques_simply, platform1-platformN）
    val yesterday_plt = sc.textFile(yesterday_input).map(_.split("\\|"))
      .map(x => {
        val ques = UnansQuesUtil.quesSimplify(x(3))
        val ans = x(5)
        val ans_type = if ( bc_unclear_q != null && bc_unclear_q.value.exists(p => ans.contains(p))) 0 else x(6).toInt
        //简化问题，回答，回答类型，平台
        (ques, ans, ans_type, x(14))
      }).filter(x => List(0, 11).contains(x._3))
      .map(x => (x._1, x._4))
      .groupByKey().mapValues(iter => iter.toList.distinct.mkString(","))
      .cache()

    //昨日未回答问题List
    val unansList = yesterday_plt.map(_._1).distinct.collect()

    val period_plts = input_data.map(_.split("\\|"))
      .map(x => {
        val ques = UnansQuesUtil.quesSimplify(x(3))
        //简化问题，回答，回答类型，平台
        (ques, x(5), x(6).toInt, x(14))
      }).map(x => {
      val ans = x._2
      val answered = if (List(0, 11).contains(x._3) || (bc_unclear_q != null && bc_unclear_q.value.exists(p => ans.contains(p)))) false
      else true
      (x._1, answered, x._4)
    }).filter(x => unansList.contains(x._1))
      .map(x => ((x._1, x._2), x._3))
      .groupByKey().mapValues(iter => iter.toList.distinct.mkString(","))
      .map(x => (x._1._1, (x._1._2, x._2)))
      .groupByKey().mapValues(iter => iter.toMap)
      .map(x => (x._1, x._2.getOrElse(true, ""), x._2.getOrElse(false, "")))
      .map(x => (x._1, (x._2, x._3)))

    // 问题、昨日未回答平台、周期内已回答平台、周期内未回答平台
    yesterday_plt.join(period_plts).map(x => List(x._1, x._2._1, x._2._2._1, x._2._2._2).mkString("|"))
      .saveAsTextFile(params.platform_info)
  }

  /**
    * 合并1天 7天 1月的转人工次数
    * @param oneday_data
    * @param week_data
    * @param month_data
    * @param params
    * @return
    */
  def mergeManualAll(oneday_data: RDD[(String, Int)], week_data: RDD[(String, Int)], month_data: RDD[(String, Int)],
                     params: Params): Unit ={
    oneday_data
      .leftOuterJoin(week_data)
      .leftOuterJoin(month_data)
      .map({case (ques, ((dcnt, wcnt), mcnt)) => {
        List(ques, dcnt, wcnt.getOrElse(0), mcnt.getOrElse(0)).mkString("|")
      }})
      .saveAsTextFile(params.manual_cnt)
  }

  /**
    * 根据问题次数、触发转人工距离、问题时间计算问题转人工的置信度、等级
    *  以下为目前评分标准：
    *   触发转人工距离==1：20分
    *   触发转人工距离<=4：10分
    *   触发转人工距离<=10：5分
    *   触发转人工距离>10：1分
    *   昨天的问句：20分
    *   3天内的问句：10分
    *   一周内的问句：5分
    *   一个月内的问句：1分
    *   评分 = 距离分 + 时间分
    * @param sc
    * @param params
    */
  def toManualWeight(sc: SparkContext, params: Params): Unit ={
    val days_list = params.analyzeDays.split(",").map(_.toInt)
    val month = days_list(2)
    val fromD = InputPathUtil.getTargetDate(params.targetDate)
    val data = sc.textFile(InputPathUtil.getInputPath(month, fromD.plusDays(1), params.inputPath))
      .map(x => {
        val line = x.split("\\|",-1)
        //(session_id, visit_time, user_id, question, answer, question_type, answer_type, faq_name, sim)
        (line(1), line(0), line(2), line(3),
          if(line(5).length > 100) line(5).substring(0, 100) + "......" else line(5),
          line(4), line(6), line(8), line(12))
      })
      .cache()

    //有人工的session(session_id, session_detail)
    val sentimentDetails = data.groupBy(_._1)
      .join(data.filter(x => isToManual(x._8, params)).map(x => (x._1, 1)).distinct())
      .map(x => (x._1, x._2._1))
      .cache()

    //未回答先剔除主动转人工
    val unanswer = sentimentDetails.filter(x => isIniti(x, params) == false)
      .filter(x => x._2.toList.exists(y => y._7 == "0"
        || y._7 == "11"
        || y._8 == " 用户描述不清楚或机器人未理解"
        || (y._8 == "电脑反问" && (if(y._9 != "") y._9.toDouble < 0.9 else true))
        || (y._8 == "手机反问" && (if(y._9 != "") y._9.toDouble < 0.7 else true))
        || (y._8 == "打印机反问" && (if(y._9 != "") y._9.toDouble < 0.7 else true))))
      .flatMap(x => x._2.toSeq)
      //(session_id, visit_time, question, answer, faq_name)
      .map(x => (x._1, x._2, x._4, x._5, x._8))
      .cache()

    //(session_id, (ask_answer_info, question_list, visit_time_list, to_manual_indexs))
    val session = unanswer
      .groupBy(_._1)
      .map(x => {
        val detail = x._2.toList.sortBy(_._2)
        (x._1, (detail.map(y => List(y._2, y._3, y._4, y._5).mkString("):(")).mkString("|"),
          detail.map(y => y._3), detail.map(y => y._2),
          detail.zipWithIndex.filter(y => isToManual(y._1._5, params)).map(y => y._2)))
      })

    val thresholdList = params.threshold.split(",").toList.map(_.toInt)
    data.filter(x => x._7 == "0"
      || x._7 == "11"
      || x._8 == " 用户描述不清楚或机器人未理解"
      || (x._8 == "电脑反问" && (if(x._9 != "") x._9.toDouble < 0.9 else true))
      || (x._8 == "手机反问" && (if(x._9 != "") x._9.toDouble < 0.7 else true))
      || (x._8 == "打印机反问" && (if(x._9 != "") x._9.toDouble < 0.7 else true)))
      .map(x => (x._1, x._4))
      .distinct()
      .join(session)
      .map(x => {
        val questionIndex = x._2._2._2.zipWithIndex.filter(y => y._1 == x._2._1).map(y => y._2)
        var minDis = Int.MaxValue
        var minIndex = Int.MaxValue
        for (i <- questionIndex) {
          for (j <- x._2._2._4) {
            val dis = if (i < j) j - i else i - j
            if(dis < minDis){
              minDis = dis
              minIndex = i
            }
          }
        }
        val lastTimeSplit = x._2._2._3(minIndex).split(" ")(0).split("-")
        val lastTime = new DateTime(lastTimeSplit(0).toInt, lastTimeSplit(1).toInt, lastTimeSplit(2).toInt, 0, 0, 0, 0)
        (x._2._1, getDistanceScore(minDis) + getTimeScore(fromD, lastTime))
      })
      .reduceByKey(_+_)
      .map(x => (x._1, getLevel(x._2, thresholdList(0), thresholdList(1), thresholdList(2))))
      .sortBy(_._2, false)
      .map(x => x._1 + "|" + x._2)
      .saveAsTextFile(params.manual_weight)
  }

  /**
    * 昨日转人工统计 or  7days or 1month
    * @param sc
    * @param manualCount
    * @return
    */
  def mergeManualCount(sc: SparkContext, manualCount:RDD[(String, Int)]): RDD[(String, Int)] ={
    val oneday_data = manualCount.map(x => {
      val ques = UnansQuesUtil.quesSimplify(x._1)
      (ques, x._2.toInt)
    }).filter(x => x._1 != null && x._1.trim.length != 0)
      .reduceByKey(_+_).cache()
    oneday_data
  }

  /**
    * Count manual by specified period
    * @param sc
    * @param params
    */
  def manualCount(sc: SparkContext, params: Params, analyzeDays: Int): RDD[(String, Int)] ={
    val fromD = InputPathUtil.getTargetDate(params.targetDate)
    val input_data = sc.textFile(InputPathUtil.getInputPath(analyzeDays, fromD.plusDays(1), params.inputPath))

    val data = input_data.map(x => {
      val line = x.split("\\|", -1)
      //(session_id, visit_time, user_id, question, answer, question_type, answer_type, faq_name, sim)
      (line(1), line(0), line(2), line(3),
        if(line(5).length > 100) line(5).substring(0, 100) + "......" else line(5),
        line(4), line(6), line(8), line(12))
    }).cache()

    // 得到转人工的会话ID
    val manualSids = data.filter(x => isToManual(x._8, params)).map(x => (x._1, 1)).reduceByKey(_ + _)
      .map(_._1).collect().toSet
    val bc_manualSids = sc.broadcast(manualSids)

    // 过滤得到转人工会话的问答记录
    // 1.包含转人工服务
    // 2.不是直接转人工
    // 3.一次会话过程中包含回答类型是0（未回答）或者11（建议问）的
    // session_id, visit_time, question, answer, faq_name
    val manualAsk = data
      .groupBy(_._1)
      .filter(x => bc_manualSids.value.contains(x._1))
      .join(data.filter(x => isToManual(x._8, params)).map(x => (x._1, 1)).distinct())
      .map(x => (x._1, x._2._1))
      .filter(x => !isDirectToManual(x._2, params))
      .filter(x => x._2.toList.exists(y => y._7 == "0" || y._7 == "11"))
      .flatMap(x => x._2.toSeq)
      .map(x => (x._1, x._2, x._4, x._5, x._8))
      .cache()

    println("============ manualAsk: session_id, visit_time, question, answer, faq_name")
    manualAsk.take(10).foreach(println)

    val questionCount = manualAsk.map(x => (x._3, x._1))
      .distinct()
      .map(x => (x._1, 1))
      .reduceByKey(_ + _)
    questionCount

    // 得到转人工会话id，会话中包含的全部问答记录，会话问题列表，转人工的位置
    // session_id, session按时间排序好的详情, session中的问题列表,
    //    val session = toMerge
    //      .groupBy(_._1)
    //      .map(x => {
    //        val detail = x._2.toList.sortBy(_._2)
    //        (x._1, (detail.map(y => List(y._2, y._3, y._4, y._5).mkString("):(")).mkString("|"),
    //          detail.map(y => y._3), detail.indexOf(detail.filter(y => y._5 == "转人工服务"))))
    //      })
    //
    //    toMerge
    //      .map(x => (x._1, x._3))
    //      .distinct()
    //      .join(session)
    //      .map {case(sid, (q, (detail, q_list, manual_idx))) => {
    //        val dis = Math.abs(q_list.indexOf(q) - manual_idx)
    //        (q, (sid, detail, dis))
    //      }}
    //      .groupByKey()
    //      .map(x => List(x._1, x._2.toList.length).mkString("|"))
    //      .saveAsTextFile(params.detailCountPath)
  }

  /**
    * 是否主动转人工
    * @param sessionDetail
    * @return
    */
  def isIniti(sessionDetail:
              (String, Iterable[(String, String, String, String, String, String, String, String, String)]), params: Params): Boolean = {
    //    val sessionId = sessionDetail._1
    val detail = sessionDetail._2.toList.sortBy(_._2)
    val firstTo = detail.indexOf(detail.filter(y => isToManual(y._8, params)).take(1)(0))
    if(firstTo != 0) detail.take(firstTo + 1).exists(y => y._7 != "8") != true else true
  }

  /**
    * 是否直接转人工
    * 1.找到转人工服务的位置
    * 2.从开始到这个位置，是否有不是聊天（answer-type: 4-通用聊天库，8-定制聊天库）的问题
    *  如果从非聊天问题导致转人工，返回false，否则认为是直接转人工
    * @param sessionDetail
    * @return
    */
  def isDirectToManual(sessionDetail :
                       Iterable[(String, String, String, String, String, String, String, String, String)], params: Params): Boolean = {
    val detail = sessionDetail.toList.sortBy(_._2)
    val firstTo = detail.map(_._8).indexOf(params.manual_faq)
    if (firstTo == 0)
      return true
    !detail.take(firstTo + 1).exists(y => !List(4, 8).contains(y._7))
  }

  // 是否是转人工
  def isToManual(faqName : String, params: Params) : Boolean = {
    faqName == params.manual_faq
  }

  def getDistanceScore(dis : Int): Int = {
    if(dis == 1) 20
    else if(dis <= 4) 10
    else if(dis <= 10) 5
    else 1
  }

  def getTimeScore(targetDate : DateTime, date : DateTime): Int = {
    val betweenDay = Days.daysBetween(date, targetDate).getDays
    if(betweenDay == 0) 20
    else if(betweenDay <= 3) 10
    else if(betweenDay <= 7) 5
    else 1
  }

  def getLevel(score : Int, th1 : Int, th2 : Int, th3 :Int): Int = {
    if(score >= th1) 3
    else if(score >= th2) 2
    else if(score >= th3) 1
    else 0
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("to manual count") {
      head("问题导致转人工次数分析")
      opt[String]("inputPath")
        .text(s"Ask数据路径")
        .action((x, c) => c.copy(inputPath = x))
      opt[String]("manual_cnt")
        .text(s"问题导致转人工次数结果路径")
        .action((x, c) => c.copy(manual_cnt = x))
      opt[String]("manual_weight")
        .text(s"问题导致转人工次数结果路径")
        .action((x, c) => c.copy(manual_weight = x))
      opt[String]("targetDate")
        .text("指定分析日期,例: 2016/01/01, 默认0表示昨天")
        .action((x, c) => c.copy(targetDate = x))
      opt[String]("analyze_days")
        .text("分析数据天数范围")
        .action((x, c) => c.copy(analyzeDays = x))
      opt[String]("dfsUri")
        .text("hadoop fs default name")
        .action((x, c) => c.copy(dfsUri = x))
      opt[Boolean]("removeMode")
        .text("remove or backup output dir that exists")
        .action((x, c) => c.copy(removeMode = x))
      opt[String]("threshold")
        .text("threshold")
        .action((x, c) => c.copy(threshold = x))
      opt[String]("unclearAnswerPath")
        .text("不能认作回答良好的答案")
        .action((x, c) => c.copy(unclearAnswerPath = x))
      opt[String]("excludeSuggestPath")
        .text("排除的问题")
        .action((x, c) => c.copy(excludeSuggestPath = x))
      opt[String]("domainWordsPath")
        .text("业务关键词")
        .action((x, c) => c.copy(domainWordsPath = x))
      opt[String]("platform_info")
        .text("output platform")
        .action((x, c) => c.copy(platform_info = x))
      opt[String]("oneDayUnanswer")
        .text("不能认作回答良好的答案")
        .action((x, c) => c.copy(oneDayUnanswer = x))
      opt[String]("suggest_hit")
        .text("suggest count")
        .action((x, c) => c.copy(suggest_hit = x))
      opt[String]("chat_list")
        .text("question_weight_by_keywords")
        .action((x, c) => c.copy(chat_list = x))
      opt[String]("manual_faq")
        .text("manual keywords")
        .action((x, c) => c.copy(manual_faq = x))
      opt[Int]("days")
        .text("analysis days")
        .action((x, c) => c.copy(days = x))
      checkConfig { params =>
        success
      }
    }
    parser.parse(args, defaultParams).map{params =>
      if (HadoopOpsUtil.exists(params.dfsUri, params.manual_cnt)) {
        if (params.removeMode) {
          HadoopOpsUtil.removeDir(params.dfsUri, params.manual_cnt)
        } else {
          HadoopOpsUtil.backupDir(params.dfsUri, params.manual_cnt)
        }
      }
      if(HadoopOpsUtil.exists(params.dfsUri, params.manual_weight))
        HadoopOpsUtil.removeDir(params.dfsUri, params.manual_weight)
      if (HadoopOpsUtil.exists(params.dfsUri, params.chat_list))
        HadoopOpsUtil.removeDir(params.dfsUri, params.chat_list)
      if (HadoopOpsUtil.exists(params.dfsUri, params.suggest_hit))
        HadoopOpsUtil.removeDir(params.dfsUri, params.suggest_hit)
      if (HadoopOpsUtil.exists(params.dfsUri, params.platform_info))
        HadoopOpsUtil.removeDir(params.dfsUri, params.platform_info)
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  case class Params(
                     inputPath : String = "/production/lenovo/data/ask",
                     unclearAnswerPath: String = "/production/lenovo/config/unclear_answer",
                     excludeSuggestPath: String = "/production/lenovo/config/exclude_faqs",
                     domainWordsPath: String = "/mnt/disk11/code/xiaoi/zzj/lenovo_similar/domain_words.txt",
                     stopWordsPath: String = "/opt/xiaoi/txt/stopwords_least.txt",
                     oneDayUnanswer: String = "/production/nlp/output/unanswered_analysis/step_filter",
                     targetDate : String = "0",
                     analyzeDays : String = "1,7,30",
                     dfsUri: String = "hdfs://172.16.0.1:9000",
                     removeMode: Boolean = true,
                     manual_cnt: String = "/production/lenovo/output/unanswered_analysis/step_manual_cnt",
                     manual_weight: String = "/production/lenovo/output/to_manual_analyze/step_to_manual_weight",
                     threshold : String = "40,30,20",
                     platform_info: String = "/production/lenovo/output/unanswered_analysis/step_platform",
                     suggest_hit: String = "/production/lenovo/output/unanswered_analysis/step_suggest_hit",
                     chat_list: String = "/production/lenovo/output/unanswered_analysis/step_chat_level",
                     manual_faq: String = "转人工服务",
                     days: Int = 30
          )

}

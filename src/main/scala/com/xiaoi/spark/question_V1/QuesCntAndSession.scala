package com.xiaoi.spark.question

import com.xiaoi.common.{HadoopOpsUtil, InputPathUtil}
import com.xiaoi.spark.util.UnansQuesUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

/**
 * 按照周期和回答类型统计问题次数以及会话详情
 * Created by ligz on 15/10/9.
 */
object QuesCntAndSession {

  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName("QuesPeriodCntByType")
    val sc = new SparkContext(conf)


    // 处理一天数据或者最近n天数据
    val fromD = InputPathUtil.getTargetDate(params.targetDate)
    val input_data = sc.textFile(InputPathUtil.getInputPath(params.days, fromD.plusDays(1), params.inputPath)).cache()
    val bc_unclear = if (HadoopOpsUtil.exists(params.dfsUri, params.unclearPath)) {
      val unclear = sc.textFile(params.unclearPath).filter(_.trim.length > 0).collect()
      sc.broadcast(unclear)
    } else null
    //sim_ques: 未回答问题、最近待处理问题、相似度、最近待处理问题类型
    val sim_ques = sc.textFile(params.simQuesPath)  //CalcSimUnansQuestions的结果


    //按照周期和回答类型统计问题次数 UnansQuesPeriodCntByType, SimUnansPeriodCntByType  ->  QuesPeriodCntByType
    //昨日未回答的相似问在周期类各回答类型的频次和会话数
    //sim_ques, 未回答问题、最近待处理问题、相似度、最近待处理问题类型
    val sim_data = sim_ques.map(_.split("\\|", -1))
      .map(x => x(params.oriQuesIdx)).distinct().collect.toList   //oriQuesIdx 1
    val bc_sim = sc.broadcast(sim_data)
    val data_sim = input_data.map(x => {
      val fields = x.split("\\|", -1)
      val ori_ques = fields(3).trim
      val ques = UnansQuesUtil.quesSimplify(ori_ques)
      val ans = fields(5)
      val ex = if (fields.length < 16) null else fields(15).toLowerCase()
      val ans_type = if (bc_unclear.value.exists(p => ans.contains(p))) 0 else fields(6).toInt
      (ques, ans_type, ex, fields(0), fields(1))
    }).filter({ case (ques, ans_type, ex, dt, sid) =>
      ex == null || ex.toLowerCase == "null" || ex.trim.length == 0
    }).filter(x => bc_sim.value.contains(x._1)).cache()

    countByType(data_sim, params.cnt_sim_unans, params.sid_sim_unans)  //step_recent_sim_unans

    //step_yesterday_unans_ori:  未回答问题，原始问题，0未回答次数，11建议问次数
    val step_yesterday_unans_ori = sc.textFile(params.quesPath)   //GetUnanswered的结果
    //昨日未回答问题在周期内各回答类型的频次和会话数
    val ques_data = step_yesterday_unans_ori.map(_.split("\\|", -1))
      .map(x => x(params.oriQuesIdx)).distinct().collect.toList   //oriQuesIdx 1
    val bc_que = sc.broadcast(ques_data)
    val data = input_data.map(x => {
      val fields = x.split("\\|", -1)
      val ori_ques = fields(3).trim
      val ques = UnansQuesUtil.quesSimplify(ori_ques)
      val ans = fields(5)
      val ex = if (fields.length < 16) null else fields(15).toLowerCase()
      val ans_type = if (bc_unclear.value.exists(p => ans.contains(p))) 0 else fields(6).toInt
      (ques, ans_type, ex, fields(0), fields(1))
    }).filter({ case (ques, ans_type, ex, dt, sid) =>
      ex == null || ex.toLowerCase == "null" || ex.trim.length == 0
    }).filter(x => bc_que.value.contains(x._1)).cache()
    countByType(data, params.cnt_unans, params.sid_unans)     //step_yesterday_unans_ori

    //根据会话ID获取会话详情
    getDetailBySid(sc, params)

    sc.stop()

  }

  /**
    * Session detail By sid
    * @param sc
    * @param params
    */
  def getDetailBySid(sc: SparkContext, params: Params): Unit ={
    val fromD = InputPathUtil.getTargetDate("0")
    val input_data = sc.textFile(InputPathUtil.getInputPath(params.days, fromD.plusDays(1), params.inputPath)).cache()

    val input_sids = sc.textFile(params.sidPath + "/*/").map(_ -> 1).collectAsMap()
    val bc_sids = sc.broadcast(input_sids)

    val data_sid = input_data.map(x => {
      val fields = x.split("\\|")
      val dt = fields(0)
      val sid = fields(1)
      //      val uid = fields(2)
      val ques = fields(3)
      // 获取回答的前100个字符
      val ans = fields(5)
      val short_ans = if (ans.length > 100) ans.take(100) + "..." else ans
      val platform = fields(14)
      val faq = fields(8)
      (dt, sid, ques, short_ans, faq, platform)
    })

    val escapeQuote = (x: String) => {
      x.replaceAll("\"", "\\\\\"")
    }

    val flt_data = data_sid.filter(x => bc_sids.value.contains(x._2))  //step_sid_detail
      .map({case (dt, sid, ques, ans, faq, platform) =>
      (sid, List[(String, String, String, String, String)]((dt, ques, ans, faq, platform)))
    }).reduceByKey(_ ++ _)
      .mapValues(iter => {
        iter.sortBy(_._1).map({ case (dt, ques, ans, faq, platform) => {
          val sidDetail = new StringBuilder
          sidDetail.append("{")
          sidDetail.append("\"visittime\":\"").append(dt)
          sidDetail.append("\",\"question\":\"").append(escapeQuote(ques))
          sidDetail.append("\",\"answer\":\"").append(escapeQuote(ans))
          sidDetail.append("\",\"faq\":\"").append(escapeQuote(faq))
          sidDetail.append("\",\"platform\":\"").append(platform)
          sidDetail.append("\"}")
          sidDetail.toString()
        }
        }).mkString("[", ",", "]")
      }).map(x => x._1 + "|" +  x._2).saveAsTextFile(params.detailPath)
  }

  /**
    * 按照周期和回答类型统计问题次数
    * @param data
    */
  def countByType(data: RDD[(String, Int, String, String, String)], cntOutPath: String, sidOutPath: String): Unit ={
    val data_1 = data.filter(_._2 == 1).cache()
    val data_0 = data.filter(_._2 == 0).cache()
    val data_11 = data.filter(_._2 == 11).cache()
    val data_u = data.filter(x => !List(1, 0, 11).contains(x._2)).cache()

    val ques_1_cnt = data_1.map(x => (x._1, 1)).reduceByKey(_ + _)
    val ques_0_cnt = data_0.map(x => (x._1, 1)).reduceByKey(_ + _)
    val ques_11_cnt = data_11.map(x => (x._1, 1)).reduceByKey(_ + _)
    val ques_u_cnt = data_u.map(x => (x._1, 1)).reduceByKey(_ + _)

    val ques_1_sids = data_1.map { case (ques, anstype, ex, dt, sid) =>
      (ques, List[(String, String)]((dt, sid)))
    }
      .reduceByKey(_ ++ _)
      .mapValues(iter => iter.sortBy(_._1).map(_._2).distinct.takeRight(5).mkString(","))
      .cache()

    val ques_0_sids = data_0.map { case (ques, anstype, ex, dt, sid) =>
      (ques, List[(String, String)]((dt, sid)))
    }
      .reduceByKey(_ ++ _)
      .mapValues(iter => iter.sortBy(_._1).map(_._2).distinct.takeRight(5).mkString(","))
      .cache()

    val ques_11_sids = data_11.map { case (ques, anstype, ex, dt, sid) =>
      (ques, List[(String, String)]((dt, sid)))
    }
      .reduceByKey(_ ++ _)
      .mapValues(iter => iter.sortBy(_._1).map(_._2).distinct.takeRight(5).mkString(","))
      .cache()


    val ques_u_sids = data_u.map { case (ques, anstype, ex, dt, sid) =>
      (ques, List[(String, String)]((dt, sid)))
    }
      .reduceByKey(_ ++ _)
      .mapValues(iter => iter.sortBy(_._1).map(_._2).distinct.takeRight(5).mkString(","))
      .cache()

    val ques_1_cnt_sids = ques_1_cnt.join(ques_1_sids)
      .map({ case (ques, (cnt, sids)) => (ques, (cnt, sids)) })

    val ques_0_cnt_sids = ques_0_cnt.join(ques_0_sids)
      .map({ case (ques, (cnt, sids)) => (ques, (cnt, sids)) })

    val ques_11_cnt_sids = ques_11_cnt.join(ques_11_sids)
      .map({ case (ques, (cnt, sids)) => (ques, (cnt, sids)) })

    val ques_u_cnt_sids = ques_u_cnt.join(ques_u_sids)
      .map({ case (ques, (cnt, sids)) => (ques, (cnt, sids)) })

    val ques_cnt_sids = data.map(x => (x._1, x._1)).distinct()
      .leftOuterJoin(ques_1_cnt_sids)
      .leftOuterJoin(ques_0_cnt_sids)
      .leftOuterJoin(ques_11_cnt_sids)
      .leftOuterJoin(ques_u_cnt_sids)
      .map({ case (ques, (((((q, q1Info), q0Info)), q11Info), quInfo)) =>
        (ques, q1Info.getOrElse((0, "")),
          q0Info.getOrElse((0, "")),
          q11Info.getOrElse((0, "")),
          quInfo.getOrElse(0, ""))
      })
    ques_cnt_sids.map({ case (ques, (cnt_1, sids_1), (cnt_0, sids_0), (cnt_11, sids_11), (cnt_u, sids_u)) =>
      List(ques, cnt_1, sids_1, cnt_0, sids_0, cnt_11, sids_11, cnt_u, sids_u).mkString("|")
    }).saveAsTextFile(cntOutPath)

    // 获取session 列表
    val sids = ques_1_sids.flatMap(x => x._2.split(","))
      .union(ques_0_sids.flatMap(x => x._2.split(",")))
      .union(ques_11_sids.flatMap(x => x._2.split(",")))
      .union(ques_u_sids.flatMap(x => x._2.split(",")))
    sids.distinct().saveAsTextFile(sidOutPath)
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("Get questions count by type in specified period") {
      head("Count questions by type in a period")
      opt[String]("simQuesPath")
        .text("Input path for sim question data")
        .action((x, c) => c.copy(simQuesPath = x))
      opt[String]("quesPath")
        .text("Input path for question data")
        .action((x, c) => c.copy(quesPath = x))
      opt[String]("unclearPath")
        .text("可以认作待处理问题的关键词路径")
        .action((x, c) => c.copy(unclearPath = x))
      opt[String]("inputPath")
        .text("Input path for all data")
        .action((x, c) => c.copy(inputPath = x))
      opt[String]("targetDate")
        .text("target date(example: 2016/01/01),default 0 for yesterday")
        .action((x, c) => c.copy(targetDate = x))
      opt[Int]('d', "days")
        .text("how many days to analysis")
        .action((x, c) => c.copy(days = x))
      opt[Int]("oriQuesIdx")
        .text("position for origin question in the input(start from 0)")
        .action((x, c) => c.copy(oriQuesIdx = x))
      opt[Int]('c', "illegalCharNum")
        .text("illegal char number")
        .action((x, c) => c.copy(illegalCharNum = x))
      opt[String]('u', "dfsUri")
        .text("hadoop fs default name")
        .action((x, c) => c.copy(dfsUri = x))
      opt[Boolean]('r', "removeMode")
        .text("remove or backup output dir that exists")
        .action((x, c) => c.copy(removeMode = x))
      opt[String]("cnt_unans")
        .text("Output path for question cnt of different type")
        .action((x, c) => c.copy(cnt_unans = x))
      opt[String]("cnt_sim_unans")
        .text("Output path for target session ids")
        .action((x, c) => c.copy(cnt_sim_unans = x))
      opt[String]("sid_unans")
        .text("Output path for question cnt of different type")
        .action((x, c) => c.copy(sid_unans = x))
      opt[String]("sid_sim_unans")
        .text("Output path for target session ids")
        .action((x, c) => c.copy(sid_sim_unans = x))
      opt[String]("sidPath")
        .text("output for sid ")
        .action((x, c) => c.copy(sidPath = x))
      opt[String]("detailPath")
        .text("session detail output")
        .action((x, c) => c.copy(detailPath = x))
      checkConfig { params => success }
    }

    parser.parse(args, defaultParams).map {params =>
      if(HadoopOpsUtil.exists(params.dfsUri, params.cnt_unans))
        HadoopOpsUtil.removeDir(params.dfsUri, params.cnt_unans)
      if (HadoopOpsUtil.exists(params.dfsUri, params.cnt_sim_unans))
        HadoopOpsUtil.removeDir(params.dfsUri, params.cnt_sim_unans)
      if (HadoopOpsUtil.exists(params.dfsUri, params.sid_unans))
        HadoopOpsUtil.removeDir(params.dfsUri, params.sid_unans)
      if(HadoopOpsUtil.exists(params.dfsUri, params.sid_sim_unans))
        HadoopOpsUtil.removeDir(params.dfsUri, params.sid_sim_unans)
      if (HadoopOpsUtil.exists(params.dfsUri, params.sidPath))
        HadoopOpsUtil.removeDir(params.dfsUri, params.sidPath)
      if (HadoopOpsUtil.exists(params.dfsUri, params.detailPath))
        HadoopOpsUtil.removeDir(params.dfsUri, params.detailPath)

      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  case class Params(
                     simQuesPath: String = "/production/lenovo/output/unanswered_analysis/step_recent_sim_unans",
                     quesPath: String = "/production/lenovo/output/unanswered_analysis/step_yesterday_unans_ori",
                     inputPath: String = "/production/lenovo/data/ask",
                     unclearPath: String = "/production/lenovo/config/unclear_answer",
                     targetDate: String = "0",
                     days: Int = 30,
                     oriQuesIdx: Int = 1,
                     cnt_unans: String = "/production/lenovo/output/unanswered_analysis/step_type_cnt/unans",
                     cnt_sim_unans: String = "/production/lenovo/output/unanswered_analysis/step_type_cnt/sim_unans",
                     sid_unans: String = "/production/lenovo/output/unanswered_analysis/step_type_sid/unans",
                     sid_sim_unans: String = "/production/lenovo/output/unanswered_analysis/step_type_sid/sim_unans",
                     sidPath: String = "/production/lenovo/output/unanswered_analysis/step_type_sid",
                     detailPath: String = "/production/lenovo/output/unanswered_analysis/step_sid_detail",
                     illegalCharNum: Int = 4,
                     dfsUri: String = "hdfs://172.16.0.1:9000",
                     removeMode: Boolean = true
                     )

}

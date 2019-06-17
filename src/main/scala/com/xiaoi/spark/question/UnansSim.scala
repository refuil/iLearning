package com.xiaoi.spark.question

import com.xiaoi.common._
import com.xiaoi.spark.{BaseOffline, MainBatch}
import com.xiaoi.spark.util.UnansQuesUtil
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object UnansSim extends BaseOffline {

  val unansType = "0"

  override def process(spark: SparkSession, params: MainBatch.Params)={

    val yesterday = InputPathUtil.getTargetDate("0")
    val recentPaths = InputPathUtil.getInputPathArray(
      params.days, yesterday.plusDays(1), params.inputPath)
    val yesterdayPath = InputPathUtil.getInputPathArray(
      1, yesterday.plusDays(1), params.inputPath)

//    val paths = Array("/production/guangda/data/ask/2019/05/01", "/production/guangda/data/ask/2019/05/02")
    //read multiple data files
    var df: DataFrame = spark.read.option("sep","|").csv(recentPaths(0))
    for (i <- 1 until recentPaths.length) {
      val df_tmp = spark.read.option("sep", "|").csv(recentPaths(i))
      if(df_tmp.count > 1) df = df.union(df_tmp)
    }

    val colNames = Seq("visit_time",
      "session_id",
      "user_id",
      "question",
      "question_type",
      "answer",
      "answer_type",
      "faq_id",
      "faq_name",
      "keyword",
      "city",
      "brand",
      "similarity",
      "module_id",
      "platform",
      "ex",
      "category",
      "nopunctuation",
      "segment",
      "sentiment")

    //0, 11
    val ansTypes = params.ansTypes.
      replaceAll("\\s+", "").split(",").toList
    // 不能认作回答良好的答案(包含:无法解答)的文件路径
    val bc_unclear = if (HDFSUtil.exists(params.dfsUri, params.unclearAnswerPath)) {
      val unclear = spark.read.textFile(params.unclearAnswerPath).filter(_.trim.length > 0).collect()
      spark.sparkContext.broadcast(unclear)
    } else null

    val bc_ignored = if (HDFSUtil.exists(params.dfsUri, params.ignoredQuesPath)) {
      val ignored_ques = spark.read.textFile(params.ignoredQuesPath)
        .map(UnansQuesUtil.ignoreQuesSimplify(_))
        .filter(_.length > 0).distinct.collect()
      spark.sparkContext.broadcast(ignored_ques)
    } else null

    def unansFilter(ques: String, ans_type: String, ex: String) = {
      ansTypes.contains(ans_type) &&
        UnansQuesUtil.quesCheck(ques, ex) &&
        (ques != null) && (ques.length > 1) &&
        ques.length > 2  && //问句长度大于2
     ignoreFilter(ques)
    }

    def ignoreFilter(ques: String) = {
      if (bc_ignored != null) {
        val short_q = UnansQuesUtil.ignoreQuesSimplify(ques)
        !bc_ignored.value.contains(short_q)
      }else true
    }

    def ansTypeForUnclear(ans: String) = {
      if (bc_unclear != null &&
        bc_unclear.value.exists(p => ans.contains(p))) "0"
    }

    import spark.implicits._

//    val unansSimCol = Seq("question", "segment")
    val recentDF = df.
      toDF(colNames: _*).
      map(x=> {
        RobotLog(
          x.getString(0),x.getString(0),
          x.getString(0),x.getString(0),
          x.getString(0),x.getString(0),
          x.getString(0),x.getString(0),
          x.getString(0),x.getString(0),
          x.getString(0),x.getString(0),
          x.getString(0),x.getString(0),
          x.getString(0),x.getString(0),
          x.getString(0),x.getString(0),
          x.getString(0),x.getString(0))
    }).as[RobotLog]
    require(recentDF.count() > 1)

    val yesterdayDF = spark.
      read.option("sep", "|").csv(yesterdayPath(0)).
      toDF(colNames: _*)
    require(yesterdayDF.count > 1)

    import spark.implicits._
    val yesUnans = yesterdayDF.
      filter($"answer_type" === unansType).
      select("question","segment")
    val recentUnans = recentDF.filter($"answer_type" === unansType).
      select("question","segment")


    import org.apache.spark.sql.functions._
    //计算未回答相似度，存储（未回答问题、最近未回答相似问、相似度、最近未回答问题类型）
    val unansSim = yesUnans.filter(length($"question") > 5).
      toDF("yestQues", "yestSeg").
      crossJoin(recentUnans.filter(length($"question") > 5)
        .toDF("recenQues","recenSeg")
      ).
//      select("question", "recenUnans", "segment", "recenSeg").
      as[SimCross].
      map(x=> (x.yestQues, x.recenQues,
        getSimilarNew(x.yestQues, x.recenQues, x.yestSeg, x.recenSeg)))


    calUnansSim(spark, yesUnans, recentUnans)

  }

  /**
    * unansQues-sim,另外一种flatmap的写法
    */
  def calUnansSim(spark: SparkSession,
                  yesUnans: DataFrame,
                  recentUnans: DataFrame)={
    import spark.implicits._
    val bc_yest = spark.sparkContext.broadcast(yesUnans.collect())
    //昨天问句，最近问句，昨天问句分词，最近问句分词
    val sim_ques = recentUnans.
      flatMap ( recenLine => {
        bc_yest.value.map { yest_q =>
          val yestQues = yest_q.getString(0)
          val yestSeg = yest_q.getString(1)
          val recenQues = recenLine.getString(0)
          val recenSeg = recenLine.getString(1)
          SimCross(yestQues, recenQues, yestSeg, recenSeg)
        }})
    sim_ques
  }

  case class RobotLog(visit_time: String,
                      session_id: String,
                      user_id: String,
                      question: String,
                      question_type: String,
                      answer: String,
                      answer_type: String,
                      faq_id: String,,
                      faq_name: String,,
                      keyword: String,
                      city: String,
                      brand: String,
                      similarity: String,
                      module_id: String,
                      platform: String,
                      ex: String,
                      category: String,
                      nopunctuation: String,
                      segment: String,
                      sentiment: String)
  case class RecenQues(ques: String, seg: String)
  case class YestQues(ques: String, seg: String)
  case class SimCross(yestQues: String,
                      recenQues: String,
                      yestSeg: String,
                      recenSeg: String
                     )
  /**
    * 计算两个句子测相似度
    * @param q1
    * @param q2
    * @return
    */
  def getSimilarNew(q1: String, q2: String,
                    q1_seg: String, q2_seg: String): Double = {
    val sim1 = CalSimilar.calEditSimilarNew(
      q1.replace(" ",""), q2.replace(" ",""))
    val sim2 = CalSimilar.calJaccardSimilar(q1_seg,q2_seg, " ")
    val maxSim = Math.max(sim1,sim2)
    val minSim = Math.min(sim1,sim2)
    val sim = if(minSim < 0.1 && maxSim<0.75){ //某一算法相似度过低时，取最小值
      minSim
    }else{
      maxSim
    }
    sim
  }


}

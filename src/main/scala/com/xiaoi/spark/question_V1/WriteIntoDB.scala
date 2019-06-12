package com.xiaoi.spark.question

import com.xiaoi.conf.ConfManager
import com.xiaoi.constant.Constants
import com.xiaoi.common.{DBUtil, DateUtil, HDFSUtil, StrUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

import scala.collection.{Map, mutable}
import util.control.Breaks._

/**
  * All Result Insert to database
  * Created by Josh on 7/18/17.
  */
object WriteIntoDB {

  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName("Write into database")
    val sc = new SparkContext(conf)

    //Seesion detail into database
    println(">>>>>Seesion detail insert>>>>>>")
    sessionDetailInsert(sc, params)

    //将新增扩展问的未回答问题概要插入Mysql
    println(">>>>>Unanswer faq insert>>>>>>")
    unanswerFaqInsert(sc, params)

    //存储未回答问题的相似问
    println(">>>>>Sim question insert>>>>>>")
    unanswerSimQuesInsert(sc, params)

    //存储未回答概要
    println(">>>>>Summery insert>>>>>>")
//    unanswerRecommInsert(sc, params)
  }

  def getStar(weight: Double): Double = {
    var re = 0.0
    if(weight >= 90)
      re = 5.0
    else if(weight >= 60)
      re = 4.0
    else if(weight >= 30)
      re = 3.0
    else if(weight >= 10)
      re = 2.0
    else re = 1.0
    re
  }

  /**
    * 存储未回答概要
    *
    * @param sc
    * @param params
    */
  def unanswerRecommInsert(sc: SparkContext, params: Params): Unit ={
    val recomm = sc.textFile(params.recomm_one_path)  //获取标准问ID和路径映射
    val recomm_cnt = sc.textFile(params.recomm_faq_cnt)  //获取推荐标准问个数映射
    val related_cnt = sc.textFile(params.related_ques_cnt) //获取相关问个数映射
    val unans_cnt = sc.textFile(params.unans_cnt)       //获取未回答问题次数映射
    val month_info = sc.textFile(params.month_info)  //获取未回答问题月度信息【已回答、未回答、建议问、其他】
    val yest_sim = sc.textFile(params.yest_sim)   //获取昨日内的未回答问题相似情况
    val chat_level = sc.textFile(params.chat_list)  //获取聊天问题
    val suggest = sc.textFile(params.suggest_hit)  //建议问的答案被点击情况
    val manual_cnt = sc.textFile(params.manual_cnt) //获取转人工次数信息
    val manual_weight = sc.textFile(params.manual_weight)  //获取转人工权重信息
    val platform = sc.textFile(params.platform_info)    //获取平台能否回答信息

    // 摘要信息
    val quesSummary = collection.mutable.Map("" -> collection.mutable.Map("" -> collection.mutable.Map("" -> 0)))

    //获取推荐标准问个数映射
    val recommCnt = getRecommCnt(recomm_cnt)

    //获取相关问个数映射
    val relatedCnt = getRelatedCnt(related_cnt)

    //获取聊天问题
    val chatInfo = getChatLevel(chat_level, quesSummary)

    //建议问的答案被点击情况
    val suggestHit = getSuggestHit(suggest, quesSummary)

    //获取转人工次数信息
    val manualCnt = getManualCnt(manual_cnt, quesSummary)

    //获取转人工权重信息
    val manualSet = getManualWeight(manual_weight, quesSummary)

    //获取平台能否回答信息
    val platformQues = getPlatformInfo(platform, quesSummary)

    //获取未回答问题月度信息【已回答、未回答、建议问、其他】
    val monthInfo = getMonthInfo(month_info)

    //获取未回答问题次数映射
    val unans_cnt_RDD = unans_cnt.map(x => {
      var (unans_ques, weight, cnt_info) = calWeight(x)

      val cnt_d0 = cnt_info.getOrElse("cnt_d_0", 0)
      val cnt_d11 = cnt_info.getOrElse("cnt_d_11", 0)
      val cnt_w0 = cnt_info.getOrElse("cnt_w_0", 0)
      val cnt_w11 = cnt_info.getOrElse("cnt_w_11", 0)
      val cnt_m0 = cnt_info.getOrElse("cnt_m_0", 0)
      val cnt_m11 = cnt_info.getOrElse("cnt_m_11", 0)

      val cnt_r = cnt_info.getOrElse("cnt_r", 0)

      // 判断未回答和建议问的突出程度
      if(cnt_d0 > UNANS_H2_D || cnt_w0 > UNANS_H2_W || cnt_m0 > UNANS_H2_M){
        quesSummary += (unans_ques -> mutable.Map("unans_cnt" ->
          mutable.Map("未回答次数很多" -> FEAT_WEI.getOrElse("unans_cnt", 0))))
      }else if(cnt_d0 > UNANS_H1_D || cnt_w0 > UNANS_H1_W || cnt_m0 > UNANS_H1_M)
        quesSummary += (unans_ques -> mutable.Map("unans_cnt" ->
          mutable.Map("未回答次数较多" -> FEAT_WEI.getOrElse("unans_cnt", 0))))
      if(cnt_d11 > SUGG_H2_D || cnt_w11 > SUGG_H2_W || cnt_m11 > SUGG_H2_M){
        quesSummary += (unans_ques -> mutable.Map("sugg_cnt" ->
          mutable.Map("建议问次数很多" -> FEAT_WEI.getOrElse("sugg_cnt", 0))))
      }else if(cnt_d11 > SUGG_H1_D || cnt_w11 > SUGG_H1_W || cnt_m11 > SUGG_H1_M)
        quesSummary += (unans_ques -> mutable.Map("sugg_cnt" ->
          mutable.Map("建议问次数较多" -> FEAT_WEI.getOrElse("sugg_cnt", 0))))

      //判断是否是建议问，并且建议问的回答是否被点击
      if(suggestHit.get(unans_ques) != None) {
        weight -= ConfManager.getDouble(Constants.WEIGHT_SUGGEST_WEI)
        weight = math.max(weight, 0.1)
      }else weight += ConfManager.getDouble(Constants.WEIGHT_SUGGEST_WEI)

      //判断是否转人工次数较多
      if(manualSet(unans_ques)){
        weight += ConfManager.getDouble(Constants.WEIGHT_MAN_WEI)
      }

      //判断是否有平台回答/不能回答反复情况
      if(platformQues.exists(_.contains(unans_ques))){
        weight += ConfManager.getDouble(Constants.WEIGHT_PLAT_WEI)
      }

      //判断是否为聊天问题或者问题只包含一个汉字,降低对应的权重
      val chat_wei = chatInfo.getOrElse(unans_ques, 0)
      if(chat_wei == -1){
        weight *= ConfManager.getDouble(Constants.WEIGHT_CHAT_WEI_N1)
      }else if(chat_wei == 0){
        weight *= ConfManager.getDouble(Constants.WEIGHT_CHAT_WEI_0)
      }else if(chat_wei == 1){
        weight *= ConfManager.getDouble(Constants.WEIGHT_CHAT_WEI_1)
      }else if(chat_wei == 2){
        weight *= ConfManager.getDouble(Constants.WEIGHT_CHAT_WEI_2)
      }
      if(unans_ques.length <= 2)
        weight = 0.1

      val unansWeight = collection.mutable.Map(unans_ques -> weight)
      val unansCnt = collection.mutable.Map(unans_ques -> cnt_info)

      //获取未回答问题所属聚类信息映射
      //clusterInfo, centerInfo, centerSet, centerWeightDict = getClusterDetail(
      //    files['cluster_detail'], unansWeight)
      //获取昨日内的未回答问题相似情况
      val (centers, nocenters) = getUnansCluster(yest_sim, unansWeight)

      //读取结果
      val lines = recomm.collect().toList

      //获取标准问ID和路径映射
      val faqPath = getFaqPath(lines)

      val addedSet = collection.mutable.Set("")
      for(line <- lines){
        val columns = line.trim().split("\\|", -1)
        val unans_ques = columns(0)
        if(nocenters.exists(_.contains(unans_ques)) || addedSet.contains(unans_ques)){
          break()
        }
        val faq = columns(1)
        val faq_id = columns(2)
        val ori_ques = columns(3)
        val weight = unansWeight.getOrElse(unans_ques, 0.0)
//        val cnt_info = unansCnt.getOrElse(unans_ques, Map("" -> 0))
        val month_info = monthInfo.getOrElse(unans_ques, "{}")
        val recomm_cnt = recommCnt.getOrElse(unans_ques, 0)
        val related_cnt = relatedCnt.getOrElse(unans_ques, 0)
        val faq_path = "/faq_path"

        //聚类信息
        val center_id = 9999
        val center_weight = 0.0
        val is_center = 1

        //摘要信息
        val summaryDict = quesSummary.getOrElse(unans_ques, Map("" -> Map("" -> 0)))
//        val summary = getTopSummary(summaryDict, 3)
        //转人工信息
        val manual_info = manualCnt.getOrElse(unans_ques, Map("" -> 0))
//        cnt_info = manual_info
        val star = getStar(weight)
        val row_id = StrUtil.uuid()
        val create_date = DateUtil.getDateBefore("auto", 1)

        //cnt_w_11": "1", "cnt_r": "1", "cnt_m_0": "0", "cnt_d_11": "1", "cnt_w_0": "0", "cnt_d_0": "0", "cnt_m_11": "1"
        val info = new StringBuilder()
        info.append("{")
        info.append("\"cnt_w_11\":\"").append(cnt_w11)
        info.append("\",\"cnt_r\":\"").append(cnt_r)
        info.append("\",\"cnt_m_0\":\"").append(cnt_m0)
        info.append("\",\"cnt_d_11\":\"").append(cnt_d11)
        info.append("\",\"cnt_11\":\"").append()
        info.append("\",\"cnt_w_0\":\"").append(cnt_w0)
        info.append("\",\"cnt_d_0\":\"").append(cnt_d0)
        info.append("\",\"cnt_m_11\":\"").append(cnt_m11)
        info.append("\"}")
//        (row_id, create_date, unans_ques, weight, faq_id, faq, faq_path,
//        recomm_cnt, related_cnt, "unhandled", ori_ques,
//        center_id, center_weight, info,
//        month_info, is_center, summary, star)
      }
    })

    //write to mysql
    val conn = DBUtil.getConnection(params.DBUrl, params.DBUser,
      params.DBPassword, ConfManager.getString(Constants.JDBC_DRIVER))
    val deleteSQL = "delete from unans_semantic where CREATE_DATE = ?"
    val sql = "insert into unans_semantic (ID, CREATE_DATE, UNANS_QUES," +
      "WEIGHT, RECOM_FAQ_ID, RECOMM_FAQ, FAQ_PATH, RECOMM_SEMANTIC, RECOMM_CNT, STATUS," +
      "CENTER_ID, CENTER_WEIGHT, CNT_INFO, MONTH_INFO, ORI_QUES, SUMMARY, STAR, IS_CENTER, MODIFY_DATE)" +
      "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    try {
      conn.setAutoCommit(false)
      //Delete data in this process
      val deletePrst = conn.prepareStatement(deleteSQL)
      deletePrst.setString(1, DateUtil.getDateBefore("auto", 1))
      deletePrst.execute()

      val wordPrst = conn.prepareStatement(sql)
      unans_cnt_RDD.collect().toList.map(x => {
//        wordPrst.setString(1, x._1)
//        wordPrst.setString(2, x._2)
//        wordPrst.setString(3, x._3)
//        wordPrst.setString(4, x._4)
//        wordPrst.setString(5, x._5)
//        wordPrst.setString(6, x._6)
//        wordPrst.setString(7, x._7)
        wordPrst.addBatch()
      })
      wordPrst.executeBatch()
    } catch {
      case e: Exception => {
        conn.rollback()
        e.printStackTrace()
      }
    } finally {
      conn.commit()
      conn.setAutoCommit(true)
      if (conn != null) conn.close()
    }

  }

  /**
    * 获取标准问ID和路径映射
    * @param lines
    */
  def getFaqPath(lines: List[String]): Unit = {
    val faqIds = collection.mutable.Set("")
    val faqPath = collection.mutable.Map("" -> "")
  }

  /**
    * 获取前n个摘要信息，摘要有权重
    * @param summaryDict
    * @param n
    */
//  def getTopSummary(summaryDict: collection.Map[String, collection.Map[String, Int]], n: Int): String ={
//    val summaryList = mutable.ListBuffer("")
//    var i = 0
//    for(Map(feat -> Map(_ -> _)) <- summaryDict){
//      summaryList += feat
//      i += 1
//      if(i == n)
//        break()
//    }
//    summaryList.toList.mkString(".")
//  }

  /**
    * 获取昨日内的未回答问题相似情况
    * @param yest_sim
    * @param unansWeight
    */
  def getUnansCluster(yest_sim: RDD[String], unansWeight: mutable.Map[String, Double]):
                        (mutable.Set[String], mutable.Set[String]) = {
    //保存问题-cid映射
    val ques_cluster_id = collection.mutable.Map("" -> "")
    //保存cid-问题列表映射
    val cluster_id_ques = collection.mutable.Map("" -> mutable.Set(""))
    //保存中心问句
    val centers = collection.mutable.Set("")
    //保存非中心问句
    var nocenters = collection.mutable.Set("")
    yest_sim.collect().toList.map(x => {
      val splits = x.trim().split("\\|", -1)
      val cluster_id = splits(0)
      val unans = splits(1)
      if(ques_cluster_id.get(unans) != None){
        ques_cluster_id += (unans -> cluster_id)
      }
      val c_id = ques_cluster_id.getOrElse(unans, "")
      val unans_set = collection.mutable.Set(unans)
      cluster_id_ques += (c_id -> unans_set)
    })

    for((_, ql) <- cluster_id_ques){
      val q_l = ql.toList
      if(q_l.length == 1){
        centers += q_l(0)
      }else{
        var c_ques = q_l(0)
        var max_wei = unansWeight.getOrElse(c_ques, 0.0)
        for(idx <- 1 to q_l.length){
          val wei_i = unansWeight.getOrElse(q_l(idx), 0.0)
          if(wei_i.>(max_wei)){
            c_ques = q_l(idx)
            max_wei = wei_i
          }
        }
        centers += c_ques
        nocenters = ql - c_ques
      }
    }
    (centers, nocenters)
  }

  /**
    * 获取未回答问题月度信息【已回答、未回答、建议问、其他】
    * @param month_info
    */
  def getMonthInfo(month_info: RDD[String]): mutable.Map[String, String] = {
    val monthInfo = collection.mutable.Map("" -> "")
    val escapeQuote = (x: String) => {
      x.replaceAll("\"", "\\\\\"")
    }
    month_info.collect().toList.map(x => {
      val splits = x.split("\\|", -1)
      val unans_ques = splits(0)
      val cnt_1 = splits(1)
      val sids_1 = splits(2)
      val cnt_0 = splits(3)
      val sids_0 = splits(4)
      val cnt_11 = splits(5)
      val sids_11 = splits(6)
      val cnt_u = splits(7)
      val sids_u = splits(8)
      val info = new StringBuilder()
      info.append("{")
      info.append("\"cnt_1\":\"").append(cnt_1)
      info.append("\",\"sids_1\":\"").append(escapeQuote(sids_1))
      info.append("\",\"cnt_0\":\"").append(escapeQuote(cnt_0))
      info.append("\",\"sids_0\":\"").append(escapeQuote(sids_0))
      info.append("\",\"cnt_11\":\"").append(escapeQuote(cnt_11))
      info.append("\",\"sids_11\":\"").append(escapeQuote(sids_11))
      info.append("\",\"cnt_u\":\"").append(escapeQuote(cnt_u))
      info.append("\",\"sids_u\":\"").append(sids_u)
      info.append("\"}")
      monthInfo += (unans_ques -> info.toString())
    })
    monthInfo
  }

  /**
    * 获取平台能否回答信息
    * @param platform
    * @param quesSummary
    */
  def getPlatformInfo(platform: RDD[String], quesSummary: mutable.Map[String, mutable.Map[String, mutable.Map[String, Int]]])
  : List[String] ={
    val platformQues = collection.mutable.ListBuffer("")
    platform.collect().toList.map(x => {
      val splits = x.trim().split("\\|", -1)
      val ques = splits(0).trim
      //昨日未回答平台信息
      val y_plts = splits(1).trim()
      //一月内问题可以回答的平台
      val m_plts = splits(2).trim()
      //一月内问题未回答的平台
      val m_splt_0 = splits(3).trim()

      if(m_plts.length != 0){
        platformQues += ques
        val pltInfo = s"当天${y_plts}平台不能回答，之前${m_plts}平台可以回答"
        quesSummary += (ques -> mutable.Map("plt_inf" -> mutable.Map(pltInfo -> FEAT_WEI.getOrElse("plt_info", 0))))
      }
    })
      platformQues.toList
  }

  /**
    * 获取转人工权重信息
    * @param manual_weight
    * @param quesSummary
    * @return
    */
  def getManualWeight(manual_weight: RDD[String], quesSummary: mutable.Map[String, mutable.Map[String, mutable.Map[String, Int]]])
  : mutable.Set[String] = {
    val manualSet = collection.mutable.Set("")
    manual_weight.collect().toList.map(x => {
      val splits = x.trim().split("\\|", -1)
      val unans_ques = splits(0)
      val weight = splits(1).toInt
      manualSet += unans_ques
      if(weight >= 2){
        quesSummary += (unans_ques -> mutable.Map("manu_wei" -> mutable.Map("容易导致转人工" ->  FEAT_WEI.getOrElse("manu_wei", 0))))
      }else
        quesSummary += (unans_ques -> mutable.Map("manu_wei" -> mutable.Map("可能导致转人工" ->  FEAT_WEI.getOrElse("manu_wei", 0))))
    })
    manualSet
  }

  /**
    * 获取转人工次数信息
    * @param manual_cnt
    * @param quesSummary
    */
  def getManualCnt(manual_cnt: RDD[String], quesSummary: mutable.Map[String, mutable.Map[String, mutable.Map[String, Int]]])
  : mutable.Map[String, mutable.Map[String, Int]] = {
    val manualCnt = collection.mutable.Map("" -> mutable.Map("" -> 0))
    manual_cnt.collect().toList.map(x => {
      val splits = x.trim().split("\\|", -1)
      val unans_ques = splits(0)
      val cnt_d = splits(1).toInt
      val cnt_w = splits(2).toInt
      val cnt_m = splits(3).toInt
      //判断转人工数量是否突出
      if(cnt_d > MANU_H2_D || cnt_w >= MANU_H2_W || cnt_m >= MANU_H2_M){
        quesSummary += (unans_ques -> mutable.Map("manu_cnt" ->
          mutable.Map("转人工次数很多" -> FEAT_WEI.getOrElse("manu_cnt", 0))))
      }else if(cnt_d >= MANU_H1_D || cnt_w >= MANU_H1_W || cnt_w >= MANU_H1_M)
        quesSummary += (unans_ques -> mutable.Map("manu_cnt" ->
          mutable.Map("转人工次数较多" -> FEAT_WEI.getOrElse("manu_cnt", 0))))
      manualCnt += (unans_ques -> mutable.Map("manual_d" -> cnt_d))
      manualCnt += (unans_ques -> mutable.Map("manual_w" -> cnt_w))
      manualCnt += (unans_ques -> mutable.Map("manual_m" -> cnt_m))
    })
    manualCnt
  }

  /**
    * 建议问的答案被点击情况
    * @param suggest
    * @param quesSummary
    */
  def getSuggestHit(suggest: RDD[String],
                    quesSummary: mutable.Map[String, mutable.Map[String, mutable.Map[String, Int]]]): mutable.Map[String, Boolean]  = {
    val suggestHit = collection.mutable.Map("" -> false)
    suggest.collect().toList.map(x => {
      val splits = x.trim().split("\\|", -1)
      val suggest_q = splits(0)
      val hit = splits(1).toFloat > ConfManager.getDouble(Constants.WEIGHT_SUGGEST_RATIO)
      if(suggestHit.get(suggest_q) == None || suggestHit.getOrElse(suggest_q, false) == false){
        suggestHit += (suggest_q -> hit)
        if(hit){
          quesSummary += (suggest_q -> mutable.Map("sugg_qlt" ->
            mutable.Map("建议问质量尚可" -> FEAT_WEI.getOrElse("sugg_qlt", 0))))
        }else{
          quesSummary += (suggest_q -> mutable.Map("sugg_qlt" ->
            mutable.Map("建议问质量不高" -> FEAT_WEI.getOrElse("sugg_qlt", 0))))
        }
      }
    })
    suggestHit
  }

  /**
    * 获取聊天问题
    * @param chat_level
    * @param quesSummary
    */
  def getChatLevel(chat_level: RDD[String],
                   quesSummary: mutable.Map[String, mutable.Map[String, mutable.Map[String, Int]]]): mutable
  .Map[String, Int] = {
    val chatInfo = collection.mutable.Map("" -> 0)
    chat_level.collect().toList.map(x => {
      val splits = x.trim().split("\\|", -1)
      val score = splits(1).toInt
      val ques = splits(0).trim
      chatInfo += (ques -> score)
      if(score == -1)
        quesSummary += (ques -> mutable.Map("chat_lvl" -> mutable.Map("偏聊天问题" -> FEAT_WEI.getOrElse("chat_lvl", 0))))
    })
    chatInfo
  }

  /**
    * 获取相关问个数映射
    * @param related_cnt
    */
  def getRelatedCnt(related_cnt: RDD[String]): mutable.Map[String, Int]  ={
    val relatedCnt = collection.mutable.Map("" -> 0)
    related_cnt.collect().toList.map(x => {
      val splits = x.trim().split("\\|", -1)
      val ques = splits(0)
      val cnt = splits(1).toInt
      relatedCnt += (ques -> cnt)
    })
    relatedCnt
  }

  /**
    * 获取推荐标准问个数映射
    * @param recomm_cnt
    */
  def getRecommCnt(recomm_cnt: RDD[String]): mutable.Map[String, Int] ={
    val recommCnt = collection.mutable.Map("" ->  0)
    recomm_cnt.collect().toList.map(x => {
      val splits = x.trim().split("\\|", -1)
      val ques = splits(0)
      val cnt = splits(1).toInt
      recommCnt += (ques -> cnt)
    })
    recommCnt
  }

  /**
    * 获得未回答次数的权重
    * @param line
    * @return
    */
  def calWeight(line: String): (String, Double, Map[String, Int]) ={
    val type_0 = ConfManager.getDouble(Constants.WEIGHT_TYPE_0)
    val type_11 = ConfManager.getDouble(Constants.WEIGHT_TYPE_11)
    val w_factor = ConfManager.getDouble(Constants.WEIGHT_W_FACTOR)
    val m_factor = ConfManager.getDouble(Constants.WEIGHT_M_FACTOR)
    val r_factor = ConfManager.getDouble(Constants.WEIGHT_R_FACTOR)
    val max_r_wei = ConfManager.getDouble(Constants.WEIGHT_MAX_R_WEI)

    val list = line.split("\\|",-1)
    var weight = 0.0
    var unans_ques = list(0)
    var cnt_info = Map{"cnt_d_0" -> list(1).toInt; "cnt_w_0" -> list(2).toInt;
      "cnt_m_0" -> list(3).toInt; "cnt_d_11" ->  list(5).toInt;
      "cnt_w_11"-> list(6).toInt; "cnt_m_11" -> list(7).toInt;
      "cnt_r" -> list(4).toInt}
    if(list.size < 8) List(unans_ques, weight)
    weight += list(1).toInt * type_0
    weight += list(2).toInt * type_0 * w_factor
    weight += list(3).toInt * type_0 * m_factor
    weight += math.min(list(4).toInt * r_factor, max_r_wei)
    weight += list(5).toInt * type_11
    weight += list(6).toInt * type_11 * w_factor
    weight += list(7).toInt * type_11 * m_factor
    weight = math.min(weight, 60)
    (unans_ques, weight, cnt_info)
  }

  /**
    * 配置次数达到什么级别，视为特征突出
    */
  //未回答问题较多
  val UNANS_H1_D = 2
  val UNANS_H1_W = 5
  val UNANS_H1_M = 10

  // 未回答问题很多
  val UNANS_H2_D = 4
  val UNANS_H2_W = 10
  val UNANS_H2_M = 20

  // 建议问较多
  val SUGG_H1_D = 2
  val SUGG_H1_W = 5
  val SUGG_H1_M = 10


  // 建议问很多
  val SUGG_H2_D = 4
  val SUGG_H2_W = 10
  val SUGG_H2_M = 20


  // 转人工较多
  val MANU_H1_D = 2
  val MANU_H1_W = 5
  val MANU_H1_M = 10


  // 转人工很多
  val MANU_H2_D = 4
  val MANU_H2_W = 10
  val MANU_H2_M = 20


  // 特征权重
  val FEAT_WEI = Map("unans_cnt" -> 7,
    "manu_wei" -> 6,
    "plt_info" -> 5,
    "sugg_qlt" -> 4,
    "sugg_cnt" -> 3,
    "manu_cnt" -> 2,
    "chat_lvl" -> 1)

  /**
    * 存储未回答问题的相似问
    * @param sc
    * @param params
    */
  def unanswerSimQuesInsert(sc: SparkContext, params: Params): Unit ={
    val unanswerSim = sc.textFile(params.unans_sim_ques_path).map(x => {
      val list = x.split("\\|",-1)
      val UNANS_QUES = list(0)
      val RELATED_QUES = list(1)
      val SIMILARITY = list(2).toDouble
      (UNANS_QUES, (RELATED_QUES, SIMILARITY))
    })
    //相似问过滤和标准问相同的问句
//    val zeroList
    val recommFaq = sc.textFile(params.unans_faq_path).map(x => {
      val list = x.split("\\|",-1)
      val UNANS_QUES = list(0)
      val RECOM_FAQ = list(1)
      (UNANS_QUES, RECOM_FAQ)
    }).aggregateByKey(mutable.MutableList[String]())(
      (list, v) => list += v,
      (list1, list2) => list1 ++= list2
    )

    //过滤去除相似问等于标准问的问句
    val filterUnansSim = unanswerSim.leftOuterJoin(recommFaq).map(x=>(x._1, x._2._1._1, x._2._1._2, x._2._2.getOrElse(null))) //未回答问句，相似问，相似度，推荐标准问list
      .filter(x=>{
      if(x._4 != null) !x._4.contains(x._2) else true
    }).map(x=>{
      (x._2,(x._1,x._3))
    })

    val monthInfo = sc.textFile(params.month_info_sim)
    val escapeQuote = (x: String) => {
      x.replaceAll("\"", "\\\\\"")
    }

    //获取相似问题月度信息【已回答、未回答、建议为、其他】
    val monthInfoRDD = monthInfo.map(x => {
      val list = x.split("\\|",-1)
      val RELATED_FAQ = list(0)
      val cnt_1 = list(1)
      val sids_1 = list(2)
      val cnt_0 = list(3)
      val sids_0 = list(4)
      val cnt_11 = list(5)
      val sids_11 = list(6)
      val cnt_u = list(7)
      val sids_u = list(8)

      val sidDetail = new StringBuilder
      sidDetail.append("{")
      sidDetail.append("\"cnt_1\":\"").append(cnt_1)
      sidDetail.append("\",\"sids_1\":\"").append(escapeQuote(sids_1))
      sidDetail.append("\",\"sids_11\":\"").append(escapeQuote(sids_11))
      sidDetail.append("\",\"cnt_u\":\"").append(escapeQuote(cnt_u))
      sidDetail.append("\",\"sids_u\":\"").append(escapeQuote(sids_u))
      sidDetail.append("\",\"cnt_0\":\"").append(escapeQuote(cnt_0))
      sidDetail.append("\",\"sids_0\":\"").append(escapeQuote(sids_0))
      sidDetail.append("\",\"cnt_11\":\"").append(cnt_11)
      sidDetail.append("\"}")
      (RELATED_FAQ, sidDetail.toString())
    }).cache()

    println("sidDetail >>>")
    monthInfoRDD.take(10).foreach(println)

    //读取相似问结果
    val unanswerSimRDD = filterUnansSim.leftOuterJoin(monthInfoRDD)
      .map(x => {
        //RDD[(String, ((String, Double), Option[String]))]
        val ID = StrUtil.uuid()
        val CREATE_DATE = DateUtil.getDateBefore("auto", 1)
        val UNANS_QUES = x._2._1._1
        val RELATED_FAQ = x._1
        val MONTHINFO = x._2._2.getOrElse("","").toString
        val ANS_FLAG = 0
        val SIMILARITY = x._2._1._2
        val STATUS = "unhandled"
        val isCenter = if(UNANS_QUES.equals(RELATED_FAQ)) "1" else "0"
        (ID, CREATE_DATE, UNANS_QUES, RELATED_FAQ, MONTHINFO, ANS_FLAG, SIMILARITY, STATUS, isCenter)
      }).filter(x => x._3 != "(,)").filter(x => x._5 != "(,)")

    println("unanswerSimRDD >>>")
    unanswerSimRDD.take(10).foreach(println)

    //write to mysql
    val conn = DBUtil.getConnection(params.DBUrl, params.DBUser,
      params.DBPassword, ConfManager.getString(Constants.JDBC_DRIVER))
    val deleteSQL = "delete from unans_related where CREATE_DATE = ?"
    val sql = "insert into unans_related (ID, CREATE_DATE, UNANS_QUES," +
      "RELATED_FAQ, MONTH_INFO, ANS_FLAG, SIMILARITY, STATUS)values (?,?,?,?,?,?,?,?)"
    try {
      conn.setAutoCommit(false)
      //Delete data in this process
      val deletePrst = conn.prepareStatement(deleteSQL)
      deletePrst.setString(1, DateUtil.getDateBefore("auto", 1))
      deletePrst.execute()

      val wordPrst = conn.prepareStatement(sql)
      unanswerSimRDD.collect().toList.map(x => {
        wordPrst.setString(1, x._1)
        wordPrst.setString(2, x._2)
        wordPrst.setString(3, x._3)
        wordPrst.setString(4, x._4)
        wordPrst.setString(5, x._5)
        wordPrst.setInt(6, x._6)
        wordPrst.setDouble(7, x._7)
        wordPrst.setString(8, x._8)
        wordPrst.addBatch()
      })
      wordPrst.executeBatch()
    } catch {
      case e: Exception => {
        conn.rollback()
        e.printStackTrace()
      }
    } finally {
      conn.commit()
      conn.setAutoCommit(true)
      if (conn != null) conn.close()
    }
  }

  /**
    * 将新增扩展问的未回答问题概要插入Mysql
    * @param sc
    * @param params
    */
  def unanswerFaqInsert(sc: SparkContext, params: Params): Unit = {
    val input_data = sc.textFile(params.unans_faq_path)
    val insertRDD = input_data.map(x => {
      val list = x.split("\\|",-1)
      val ID = StrUtil.uuid()
      val CREATE_DATE = DateUtil.getDateBefore("auto", 1)
      val UNANS_QUES = list(0)
      val RECOM_FAQ_ID = list(3)
      val RECOM_FAQ = list(1)
      val FAQ_IDX = list(2).toInt
      val FAQ_PATH = "/faq_path"

      (ID, CREATE_DATE, UNANS_QUES, RECOM_FAQ_ID, RECOM_FAQ, FAQ_IDX, FAQ_PATH)
    })

    //write to mysql
    val conn = DBUtil.getConnection(params.DBUrl, params.DBUser,
      params.DBPassword, ConfManager.getString(Constants.JDBC_DRIVER))
    val deleteSQL = "delete from unans_faq where CREATE_DATE = ?"
    val sql = "insert into unans_faq (ID, CREATE_DATE, UNANS_QUES," +
      "RECOM_FAQ_ID, RECOM_FAQ, FAQ_IDX, FAQ_PATH)values (?,?,?,?,?,?,?)"
    try {
      conn.setAutoCommit(false)
      //Delete data in this process
      val deletePrst = conn.prepareStatement(deleteSQL)
      deletePrst.setString(1, DateUtil.getDateBefore("auto", 1))
      deletePrst.execute()

      val wordPrst = conn.prepareStatement(sql)
      insertRDD.collect().toList.map(x => {
        wordPrst.setString(1, x._1)
        wordPrst.setString(2, x._2)
        wordPrst.setString(3, x._3)
        wordPrst.setString(4, x._4)
        wordPrst.setString(5, x._5)
        wordPrst.setInt(6, x._6)
        wordPrst.setString(7, x._7)
        wordPrst.addBatch()
      })
      wordPrst.executeBatch()
    } catch {
      case e: Exception => {
        conn.rollback()
        e.printStackTrace()
      }
    } finally {
      conn.commit()
      conn.setAutoCommit(true)
      if (conn != null) conn.close()
    }
  }

  def sessionDetailInsert(sc: SparkContext, params: Params): Unit = {
    val input_data = sc.textFile(params.session_detail_path)
    val insertRDD = input_data.map(x => {
      val ID = StrUtil.uuid()
      val CREATE_DATE = DateUtil.getDateBefore("auto", 1)
      val PARENT_ID = ""
      val SID = x.split("\\|",-1)(0)
      val CONTENT = x.split("\\|",-1)(1)
      (ID, CREATE_DATE, PARENT_ID, SID, CONTENT)})

    //write to mysql
    val conn = DBUtil.getConnection(params.DBUrl, params.DBUser,
      params.DBPassword, ConfManager.getString(Constants.JDBC_DRIVER))
    val deleteSQL = "delete from session_detail where CREATE_DATE = ?"
    val sql = "insert into session_detail(ID, CREATE_DATE, PARENT_ID, " +
      "SID, CONTENT) values (?,?,?,?,?)"
    try {
      conn.setAutoCommit(false)
      //Delete data in this process
      val deletePrst = conn.prepareStatement(deleteSQL)
      deletePrst.setString(1, DateUtil.getDateBefore("auto", 1))
      deletePrst.execute()

      val wordPrst = conn.prepareStatement(sql)
      insertRDD.collect().toList.map(x => {
        wordPrst.setString(1, x._1)
        wordPrst.setString(2, x._2)
        wordPrst.setString(3, x._3)
        wordPrst.setString(4, x._4)
        wordPrst.setString(5, x._5)
        wordPrst.addBatch()
      })
      wordPrst.executeBatch()
    } catch {
      case e: Exception => {
        conn.rollback()
        e.printStackTrace()
      }
    } finally {
      conn.commit()
      conn.setAutoCommit(true)
      if (conn != null) conn.close()
    }
  }

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("Write database") {
      head("Write into DB")
      opt[String]('d', "session_detail_path")
        .text("Input path for question data")
        .action((x, c) => c.copy(session_detail_path = x))
      opt[String]('e', "unans_faq_path")
        .text("Input path for question data")
        .action((x, c) => c.copy(unans_faq_path = x))
      opt[String]('f', "unans_sim_ques_path")
        .text("Input path for question data")
        .action((x, c) => c.copy(unans_sim_ques_path = x))
      opt[String]('g', "month_info_sim")
        .text("Input path for question data")
        .action((x, c) => c.copy(month_info_sim = x))
      opt[String]("recomm_one_path")
        .action((x, c) => c.copy(recomm_one_path = x))
      opt[String]("recomm_faq_cnt")
        .action((x, c) => c.copy(recomm_faq_cnt = x))
      opt[String]("related_ques_cnt")
        .action((x, c) => c.copy(related_ques_cnt = x))
      opt[String]("unans_cnt")
        .action((x, c) => c.copy(unans_cnt = x))
      opt[String]('g', "month_info")
        .action((x, c) => c.copy(month_info = x))
      opt[String]("yest_sim")
        .action((x, c) => c.copy(yest_sim = x))
      opt[String]("chat_list")
        .action((x, c) => c.copy(chat_list = x))
      opt[String]("suggest_hit")
        .action((x, c) => c.copy(suggest_hit = x))
      opt[String]("manual_cnt")
        .action((x, c) => c.copy(manual_cnt = x))
      opt[String]("manual_weight")
        .action((x, c) => c.copy(manual_weight = x))
      opt[String]("platform_info")
        .action((x, c) => c.copy(platform_info = x))
      opt[String]('o', "output_path")
        .text("output to hdfs")
        .action((x, c) => c.copy(output_path = x))
      opt[String]('a' ,"DBHost")
        .text("dbhost")
        .action((x, c) => c.copy(DBHost = x))
      opt[String]('y', "DBUser")
        .text("DBUser")
        .action((x, c) => c.copy(DBUser = x))
      opt[String]('m', "DBPassword")
        .text("db password")
        .action((x, c) => c.copy(DBPassword = x))
      opt[String]('s', "DBName")
        .text("DBName")
        .action((x, c) => c.copy(DBName = x))
      opt[String]('p', "DBUrl")
        .text("db port")
        .action((x, c) => c.copy(DBUrl = x))
      opt[String]('u', "dfsUri")
        .text("dfs url")
        .action((x, c) => c.copy(dfsUri = x))
      checkConfig { params => success }
    }

    parser.parse(args, defaultParams).map {params =>
      if (HDFSUtil.exists(params.dfsUri, params.output_path)) {
        HDFSUtil.removeDir(params.dfsUri, params.output_path)
      }
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  case class Params(
                     session_detail_path: String = "/production/lenovo/output/unanswered_analysis/step_sid_detail",
                     unans_faq_path: String = "/production/lenovo/output/unanswered_analysis/step_recent_sim_unans", //将新增扩展问的未回答问题概要插入Mysql
                     unans_sim_ques_path: String = "/production/lenovo/output/unanswered_analysis/step_recent_sim_unans",//存储未回答问题的相似问
                     month_info_sim: String = "/production/lenovo/output/unanswered_analysis/step_type_cnt/sim_unans",
                     recomm_one_path: String = "/production/lenovo/output/unanswered_analysis/step_unans_recomm/recommend_one", //存储未回答概要
                     recomm_faq_cnt: String = "/production/lenovo/output/unanswered_analysis/step_unans_recomm/recommend_cnt",
                     related_ques_cnt: String = "/production/lenovo/output/unanswered_analysis/step_unans_related_cnt",
                     unans_cnt: String = "/production/lenovo/output/unanswered_analysis/step_cnt/all",
                     month_info: String = "/production/lenovo/output/unanswered_analysis/step_type_cnt/unans",
                     yest_sim: String = "/production/lenovo/output/unanswered_analysis/step_yest_sim_unans",
                     chat_list: String = "",
                     suggest_hit: String = "",
                     manual_cnt: String = "",
                     manual_weight: String = "",
                     platform_info: String = "",
                     output_path: String = "",  //Local Output
                     DBHost: String = "",
                     DBUser: String = "",
                     DBPassword: String = "",
                     DBName: String = "",
                     DBUrl: String = "jdbc:MySQL://master:3306/robot",
                     dfsUri: String = "hdfs://172.16.0.1:9000"
                   )

}

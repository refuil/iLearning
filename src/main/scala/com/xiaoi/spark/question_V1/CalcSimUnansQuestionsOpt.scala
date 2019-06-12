package com.xiaoi.spark.question

import com.xiaoi.common.{CalSimilar, HDFSUtil, Segment}
import com.xiaoi.spark.util.UnansQuesUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

import scala.collection.{immutable, mutable}
import scala.io.Source

/**
  * 计算昨日未回答问题与一段时间内的待处理问题的相似度
  * Created by ligz on 16/04/26.
  */
object CalcSimUnansQuestionsOpt {

  /**
    * 计算未回答相似度，存储
    * @param unans_type
    * @param bc_yest
    * @param params
    * @param domainMap
    */
  def executeCalculate(unans_type: RDD[(String,String,String)], bc_yest: Broadcast[List[(String, String)]],
                       params: Params, domainMap: Map[String,Int], sc: SparkContext):RDD[(String, String, Double, String)] = {
    val similarity = params.similarity
    val shortQuesLen = params.shortQuesLen
    val highSimilarity = params.highSimilarity
    val outputPath = params.outputPath

    val parttern = "[a-zA-Z0-9]+[-|_][a-zA-Z0-9]+".r //字母数字的版本号xxx-xxx
    val parttern2 = "([a-zA-Z]+[0-9]+\\w*)|([0-9]+[a-zA-Z]+\\w*)".r //字母数字混合的关键字
    // 未回答问题、最近未回答相似问、相似度、最近未回答问题类型
    val sim_ques = unans_type.repartition(16).flatMap(recent_q => bc_yest.value.map(yest_q => {
      val yestQues = yest_q._1
      val ques_seg = yest_q._2
      (yestQues,recent_q._1,recent_q._2,ques_seg,recent_q._3) //昨天问句，最近问句，最近状态，昨天问句分词，最近问句分词
    })).filter(x=>{
      val yestQues = x._1
      val recentQues = x._2
      val yestQues_seg = x._4
      //产品型号版本号等
      val rex1 = parttern.findFirstIn(yestQues).getOrElse("none")
      val rex2 = parttern.findFirstIn(recentQues).getOrElse("none")
      //英文名词
      val reg1 = parttern2.findFirstIn(yestQues.replace(" ","")).getOrElse("none")
      val reg2 = parttern2.findFirstIn(recentQues.replace(" ","")).getOrElse("none")
      val isLetterEquals = if(!"none".equals(reg1) && !"none".equals(reg2)) reg1.equals(reg2) else true
      //产品型号要求相等，英文名词相等，昨日问句长度大于2，昨日问句不等于最近问句，昨日问句不能只有一个词
      rex1.equals(rex2) && isLetterEquals && yestQues.length>2 && yestQues_seg.contains(" ") // && !yestQues.equals(recentQues) 允许相等
    }).map(x=>{
      val yestQues = x._1
      val recentQues = x._2
      val yestQues_seg = x._4
      val recentQues_seg = x._5
      val sim = getSimilarNew(yestQues, recentQues, yestQues_seg, recentQues_seg, domainMap)
      (yestQues, recentQues, sim, x._3)
    }).filter(x=>{
      val lengthCompare = x._1.length.toDouble/x._2.length.toDouble
      if(x._1.length < shortQuesLen){
        x._3 > highSimilarity
      }else if(lengthCompare<0.5 || lengthCompare >2){ //两个问句长度相差一倍时，相似度阈值调高
        x._3 > 0.8
      }else{
        x._3 > 0.5
      }
    }).cache()

    //将相似问存储HashSet
    val quesSimilar = sim_ques.map(x=>(x._1, x._2)).aggregateByKey(mutable.HashSet[String]())(
      (set, v) => set += v ,
      (set1, set2) => set1 ++= set2
    ).map(x=>(x._1, x._2, x._2.size)).sortBy(_._3).cache()

    val bcQuesSimlar = sc.broadcast(quesSimilar.collect().toList)

    //判断当前问句是否是其他问句的相似问，并且当前问句的相似问数量更少，则删除当前问句
    val quesCnt = quesSimilar.map(x=>(x._1, x._3)).distinct().cache
    val bcQuesCnt = sc.broadcast(quesCnt.collect.toList)

    val filters = quesCnt.flatMap(x=>{
      bcQuesSimlar.value.map(y=>{
        if(y._2.contains(x._1) && !y._1.equals(x._1) && y._3 >= x._2){
          (immutable.HashSet(x._1,y._1).mkString("|"), x._1)
        } else {
          ("","")
        }
      }).filter(_._1.length > 0)
    }).foldByKey("")((a,b)=> b).map(_._2).distinct().collect().toList

    val uniqSimQues = sim_ques.filter(x=> !filters.contains(x._1)).cache()

    uniqSimQues.map(x => List(x._1, x._2, x._3, x._4).mkString("|"))
      .saveAsTextFile(outputPath)

    uniqSimQues
  }

  /**
    * 计算两个句子测相似度,编辑距离和Jaccard距离取高的
    *
    * @param q1
    * @param q2
    * @param stopFile
    * @return
    */
  def getSimilar(q1: String, q2: String, stopFile: String = ""): Double = {
    val ed = CalSimilar.calEditSimilar(q1.replace(" ", ""), q2.replace(" ", ""))
    val seg1 = Segment.segment(q1, " ")
    val seg2 = Segment.segment(q2, " ")
    val jc = CalSimilar.calJaccardSimilar(seg1, seg2, " ")
    scala.math.max(ed, jc)
  }

  /**
    * 获取一个月未回答问句，（未处理问题,回答类型，分词问句）
    * @param recentUnans
    * @param sc
    * @param stopFilePath
    * @param bc_domainList
    * @return
    */
  def getRecentUnans(recentUnans: String, sc: SparkContext, stopFilePath: String, bc_domainList:Broadcast[List[String]]):RDD[(String, String, String)] = {
    val unans_type = sc.textFile(recentUnans).mapPartitions(x=>{
      if(stopFilePath != null && !"".equals(stopFilePath) ) Segment.init(true, stopFilePath, false, "")
      if(bc_domainList != null) Segment.loadWords(bc_domainList.value)
      x
    }).map(x => {
      val fields = x.split("\\|", -1)
      val ques = fields(0).trim
      val q_type = fields(1)
      // 未处理问题,回答类型
      (ques, q_type)
    }).map(x=>{
      // 未处理问题,回答类型，分词问句
      //      (x._1,x._2,Segment.segment(x._1," "))
      (x._1,x._2,Segment.dicSegment(x._1," "))
    }).distinct().cache()
    unans_type
  }

  /**
    * 获取昨天未回答问句
    * @param yesterdayUnans
    * @param sc
    * @param stopFilePath
    * @param bc_domainList
    * @return
    */
  def getYesterdayUnans(yesterdayUnans: String, sc: SparkContext, stopFilePath: String, bc_domainList:Broadcast[List[String]]):RDD[(String,String)] = {
    val unans = sc.textFile(yesterdayUnans).mapPartitions(x=>{
      if(stopFilePath != null && !"".equals(stopFilePath) ) Segment.init(true, stopFilePath, false, "")
      if(bc_domainList != null) Segment.loadWords(bc_domainList.value)
      x
    }).map(x => {
      val fields = x.split("\\|", -1)
      val yest_ques = fields(0).trim
      //      val segment = Segment.segment(yest_ques, " ")
      val segment = Segment.dicSegment(yest_ques.replace(" ",""), " ")
      //昨日问句，昨日频次，问句分词
      (yest_ques, segment)
    }).cache()
    unans
  }

  /**
    * 对一个月未回答相似问用昨日未回答过滤，分组添加索引
    * @param unans
    * @param recentSimUnans
    * @param sc
    */
  @deprecated("逻辑变更，弃用，使用新方法unansSimQuesIndexNew zzj 2017-9-19", "1.0.0")
  def unansSimQuesIndex(unans: RDD[(String,String)], recentSimUnans: RDD[(String, String, Double, String)],
                        outputPath: String, relatedCntPath: String, sc: SparkContext):Unit = {

    val yester_data = unans.map(_._1).collect().toList
    val bc_yester_data = sc.broadcast(yester_data)
    //
    //    //对一个月未回答相似问用昨日未回答过滤，分组添加索引
    val recent_data = recentSimUnans.map(x => {
      //      //未回答问题，一个月未回答相似问
      x._1 -> x._2
    }).map(x => x._1 -> UnansQuesUtil.quesSimplify(x._2))
      .filter(x => bc_yester_data.value.contains(x._2))
      .groupBy(_._1).mapValues(iter => iter.map(_._2).toSet)
      .map(x => x._2 ++ Set(x._1))
      .zipWithIndex().map(_.swap)
      .flatMapValues(x => x)
      .cache()
    // save similar questions （index, ques）
    recent_data.map(x => List(x._1, x._2).mkString("|")).saveAsTextFile(outputPath)
    // save similar questions count info （index, cnt）
    recent_data.map(_._1 -> 1).reduceByKey(_ + _).map(x => x._1 + "|" + x._2).saveAsTextFile(relatedCntPath)
  }

  /**
    * 存储相似问非中心问句
    * @param unans
    * @param recentSimUnans
    * @param sc
    */
  def unansSimQuesIndexNew(unans: RDD[(String,String)], recentSimUnans: RDD[(String, String, Double, String)],
                           outputPath: String, sc: SparkContext):Unit = {

    val yester_data = unans.map(_._1).collect().toList
    val bc_yester_data = sc.broadcast(yester_data)
    //对一个月未回答相似问用昨日未回答过滤，分组添加索引
    val recent_data = recentSimUnans.map(x => {
      //未回答问题，一个月未回答相似问
      x._1 -> x._2
    }).filter(x => bc_yester_data.value.contains(x._2)).cache()

    val centers = recent_data.map(_._1).distinct()
    val noCenters = recent_data.flatMap(x=>Set(x._1, x._2)).distinct().subtract(centers)

    // save nocenters questions （ques）
    noCenters.saveAsTextFile(outputPath)
  }

  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName("CalcSimUnansQuestionsOpt")
    val sc = new SparkContext(conf)

    val yesterdayUnans = params.yesterdayUnans
    val recentUnans = params.recentUnans
    val stopFilePath = params.stopFilePath
    val domainPath = params.domainPath
    val outputPath = params.outputPath
    val similaryIndexPath = params.similaryIndexPath
    val relatedCntPath = params.relatedCntPath
    val removeMode = params.removeMode
    val dfsUri = params.dfsUri
    val similarity = params.similarity
    val shortQuesLen = params.shortQuesLen
    val highSimilarity = params.highSimilarity
    val useLucene = params.useLucene

    HDFSUtil.removeOrBackup(removeMode, dfsUri , outputPath )
    HDFSUtil.removeOrBackup(removeMode, dfsUri , similaryIndexPath )
    HDFSUtil.removeOrBackup(removeMode, dfsUri , relatedCntPath )

    val domainList = Source.fromFile(domainPath).getLines().toList.map(_.toLowerCase()).filter(_.trim.length > 0)
    var map:Map[String,Int] = Map()
    domainList.foreach(domain=>{
      map += (domain -> 3)
    })
    val domainMap = map
    val bc_domainList = sc.broadcast(domainList)

    //获取一个月未回答问句，（未处理问题,回答类型，分词问句）
    val unans_type = getRecentUnans(recentUnans, sc, stopFilePath, bc_domainList)

    //获取昨天未回答问句，（昨日问句，昨日频次，问句分词）
    val unans = getYesterdayUnans(yesterdayUnans, sc, stopFilePath, bc_domainList)
    val bc_yest = sc.broadcast(unans.collect().toList)

    //计算未回答相似度，存储（未回答问题、最近未回答相似问、相似度、最近未回答问题类型）
    val recentSimUnans =
      executeCalculate(unans_type, bc_yest, params, domainMap, sc)

    //取昨日未回答相似问并分组，添加索引index，存储两个结果 （index, ques） （index, cnt）
    unansSimQuesIndexNew(unans, recentSimUnans, similaryIndexPath, sc)

    sc.stop()
  }

  /**
    * 计算两个句子测相似度
    *
    * @param q1
    * @param q2
    * @param domainMap
    * @return
    */
  def getSimilarNew(q1: String, q2: String, q1_seg: String, q2_seg: String, domainMap: Map[String,Int] ): Double = {
    val sim1 = CalSimilar.calEditSimilarNew(q1.replace(" ",""),q2.replace(" ",""))
    val sim2 = CalSimilar.domainJaccardSimilar(q1_seg,q2_seg,domainMap)
    val maxSim = Math.max(sim1,sim2)
    val minSim = Math.min(sim1,sim2)
    val sim = if(minSim < 0.1 && maxSim<0.75){ //某一算法相似度过低时，取最小值
      minSim
    }else{
      maxSim
    }
    sim
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("Filter Unanswered Questions in Yesterday") {
      head("Filter Unanswered Questions in latest-N days")
      opt[String]('y', "yesterday-unanswered")
        .text("昨日待处理问题路径")
        .action((x, c) => c.copy(yesterdayUnans = x))
      opt[String]('e', "recentUnans")
        .text("最近(30天或指定天数)未处理问题路径")
        .action((x, c) => c.copy(recentUnans = x))
      opt[String]('o', "output")
        .text("相似待处理问题输出路径")
        .action((x, c) => c.copy(outputPath = x))
      opt[String]("similaryIndexPath")
        .text("昨日相似待处理问题输出路径")
        .action((x, c) => c.copy(similaryIndexPath = x))
      opt[String]("relatedCntPath")
        .text("Path for unanswered questions related questions count")
        .action((x, c) => c.copy(relatedCntPath = x))
      opt[String]("stopFilePath")
        .text("停用词路径")
        .action((x, c) => c.copy(stopFilePath = x))
      opt[String]("domainPath")
        .text("词路径")
        .action((x, c) => c.copy(domainPath = x))
      opt[Double]('s', "similarity")
        .text("相似度阀值")
        .action((x, c) => c.copy(similarity = x))
      opt[String]('u', "dfsUri")
        .text("HDFS根路径")
        .action((x, c) => c.copy(dfsUri = x))
      opt[Boolean]("removeMode")
        .text("remove or backup output dir that exists")
        .action((x, c) => c.copy(removeMode = x))
      opt[Int]("shortQuesLen")
        .text("短问题长度")
        .action((x, c) => c.copy(shortQuesLen = x))
      opt[Double]("highSim")
        .text("短问题对应的相似度阀值")
        .action((x, c) => c.copy(highSimilarity = x))
      opt[Boolean]("useLucene")
        .text("是否使用lucene")
        .action((x, c) => c.copy(useLucene = x))
      opt[Int]("luceneTopN")
        .text("lucene检索数量")
        .action((x, c) => c.copy(luceneTopN = x))
      checkConfig { params => success }

    }

    parser.parse(args, defaultParams).map {
      params => run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  case class Params(
                     yesterdayUnans: String = "",
                     recentUnans: String = "",
                     stopFilePath: String = "/opt/xiaoi/txt/stopwords_least.txt",
                     domainPath: String = "domain_words.txt",
                     similarity: Double = 0.5,
                     shortQuesLen: Int = 5,
                     highSimilarity: Double = 0.6,
                     outputPath: String = "",
                     similaryIndexPath: String = "",
                     relatedCntPath: String = "",
                     dfsUri: String = "hdfs://172.16.0.1:9000",
                     useLucene: Boolean = false,
                     luceneTopN: Int = 100,
                     removeMode: Boolean = true)

}

package com.xiaoi.spark.question

import com.xiaoi.common.{CalSimilar, HDFSUtil, LuceneUtil, Segment}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

import scala.io.Source

/**
 * 计算昨日未回答问题与一段时间内的问题的相似度
 * Created by ligz on 15/10/10.
 */
object CalcSimQuestionsOpt {

  /**
    * 获取最近一月问句
    * @param recentQuesPath
    * @param sc
    * @param stopFilePath
    * @param bc_domainList
    * @return
    */
  def getMonthQuestions(recentQuesPath: String, sc: SparkContext, stopFilePath: String, bc_domainList:Broadcast[List[String]]):RDD[(String, Int, String, Boolean, String)] = {
    val ques_cnt_faq_ans = sc.textFile(recentQuesPath).mapPartitions(x=>{
      if(stopFilePath != null && !"".equals(stopFilePath) ) Segment.init(true, stopFilePath, false, "")
      if(bc_domainList != null) Segment.loadWords(bc_domainList.value)
      x
    }).map(x => {
      val fields = x.split("\\|")
      val ques = fields(0)
      val ques_cnt = fields(1).toInt
      val faq = fields(3)
      val answered = fields(4).toBoolean
      // 简洁问题、次数、知识点、已回答标志、问句分词
      (ques, ques_cnt, faq, answered, Segment.dicSegment(ques," "))
    }).distinct()
    ques_cnt_faq_ans
  }

  /**
    * 获取昨天未回答问句
    * @param yesterdayUnans
    * @param sc
    * @param stopFilePath
    * @param bc_domainList
    * @return
    */
  def getYesterdayUnans(yesterdayUnans: String, sc: SparkContext, stopFilePath: String, bc_domainList:Broadcast[List[String]]):RDD[(String,String,String)] = {
    val unans = sc.textFile(yesterdayUnans).mapPartitions(x=>{
      if(stopFilePath != null && !"".equals(stopFilePath) ) Segment.init(true, stopFilePath, false, "")
      if(bc_domainList != null) Segment.loadWords(bc_domainList.value)
      x
    }).map(x => {
      val fields = x.split("\\|")
      val yest_ques = fields(0)
      val segment = Segment.dicSegment(yest_ques, " ")
      //昨日问句，昨日频次，问句分词
      (yest_ques, fields(1), segment)
    })
    unans
  }

  /**
    * 计算相似度
    * @param recent_ques
    * @param yest_unans
    * @param domainMap
    * @param shortQuesLen
    * @param highSimilarity
    * @return
    */
  def execCalculate(recent_ques: RDD[(String, Int, String, Boolean, String)], yest_unans: Broadcast[List[(String,String,String)]],
                    domainMap: Map[String,Int], shortQuesLen: Int, highSimilarity: Double):RDD[(String, String, Double, String, String, Int, Boolean)] = {
    val parttern = "[a-zA-Z0-9]+[-|_][a-zA-Z0-9]+".r //字母数字的版本号xxx-xxx
    val parttern2 = "([a-zA-Z]+[0-9]+\\w*)|([0-9]+[a-zA-Z]+\\w*)".r //字母数字混合的关键字
    val sim_ques_all = recent_ques.repartition(16).flatMap(recent=>{
      yest_unans.value.map(yest=>{
        //昨天问句，最近问句，昨日频次，昨日分词，最近次数、最近知识点、最近已回答标志、最近问句分词
        (yest._1, recent._1, yest._2, yest._3, recent._2, recent._3, recent._4, recent._5 )
      })
    }).filter(x=>{
      val yestQues = x._1
      val recentQues = x._2
      val yestQues_seg = x._4
      //产品型号版本号等
      val rex1 = parttern.findFirstIn(yestQues).getOrElse("none")
      val rex2 = parttern.findFirstIn(recentQues).getOrElse("none")
      //英文名词
      val reg1 = parttern2.findFirstIn(yestQues.trim.replace(" ","")).getOrElse("none")
      val reg2 = parttern2.findFirstIn(recentQues.trim.replace(" ","")).getOrElse("none")
      val isLetterEquals = if(!"none".equals(reg1) && !"none".equals(reg2)) reg1.equals(reg2) else true
      //产品型号要求相等，英文名词相等，昨日问句长度大于2，昨日问句不等于最近问句，昨日问句不能只有一个词
      rex1.equals(rex2) && isLetterEquals && yestQues.length>2 && !yestQues.trim.equals(recentQues.trim) && yestQues_seg.contains(" ")
    }).map(x=>{
      val yestQues = x._1
      val recentQues = x._2
      val yestQues_seg = x._4
      val recentQues_seg = x._8
      val sim = getSimilarNew(yestQues, recentQues, yestQues_seg, recentQues_seg, domainMap)
      //昨天问句，最近问句，相似度，昨日频次，最近知识点，最近次数、最近已回答标志
      (yestQues, recentQues, sim, x._3, x._6, x._5, x._7)
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
    sim_ques_all
  }

  def luceneCalculate(recent_ques: RDD[(String, Int, String, Boolean, String)], yest_unans: RDD[(String,String,String)],
                    shortQuesLen: Int, highSimilarity: Double, sc: SparkContext, luceneTopN: Int=100):RDD[(String, String, Double, String, String, Int, Boolean)] = {

    val ques_cnt_faq_ans = recent_ques.map(x => (x._1, x._2, x._3, x._4)).distinct().cache()

    val unans = yest_unans.map(x => (x._1, x._2)).cache()

    //Test3
    val requst_q = ques_cnt_faq_ans.map(x=>x._1)
    val unans_q = unans.map(x=>x._1)
    val lucKey = "simquestion"
    val indexWriter = LuceneUtil.getIndexWriter
    requst_q.collect().toList.foreach(x=>{
      indexWriter.addDocument(LuceneUtil.setDoc(lucKey,x))
    })
    indexWriter.close()
    val lcnres = LuceneUtil.qusQuerybyKey(lucKey,unans_q.collect().toList,luceneTopN)
    //yest , recent , sim
    val simQus = sc.parallelize(lcnres).map(x=>{
      val sim = getSimilar(x._1,x._2)
      (x._1,(x._2,sim))
    }).cache
    //recent，（yest，sim，yest_cnt）
    val yestJoin = unans.join(simQus).map(x=>(x._2._2._1,(x._1,x._2._2._2,x._2._1)))
    val allJoin =ques_cnt_faq_ans.map(x=>(x._1,(x._2,x._3,x._4))).join(yestJoin)
      .map(x=>(x._2._2._1,x._1,x._2._2._2,x._2._2._3,x._2._1._2,x._2._1._1,x._2._1._3))

    allJoin
  }

  def getSimilar(q1: String, q2: String, stopFile: String = ""): Double = {
    val ed = CalSimilar.calEditSimilar(q1.replace(" ", ""), q2.replace(" ", ""))
    val seg1 = Segment.segment(q1, " ")
    val seg2 = Segment.segment(q2, " ")
    val jc = CalSimilar.calJaccardSimilar(seg1, seg2, " ")
    scala.math.max(ed, jc)
  }

  /**
    * 存储相似度结果
    * @param sim_ques_all
    * @param outputPath
    * @param recommQuesPath
    * @param shortQuesLen
    * @param highSimilarity
    * @param similarity
    */
  def saveSimilarQues(sim_ques_all: RDD[(String, String, Double, String, String, Int, Boolean)],
                      outputPath: String, recommQuesPath: String, shortQuesLen: Int, highSimilarity: Double, similarity: Double): Unit = {
    sim_ques_all.map(x => x._1 + "|" + x._2 + "|" + x._3 + "|" + x._4 + "|" + x._5 + "|" + x._6)
      .saveAsTextFile(outputPath + "/all")

    val knn_sample = sim_ques_all.filter(x => {
      x._7 && (if (x._1.length > shortQuesLen) x._3 > similarity else x._3 > highSimilarity)
    }).filter(_._5.trim.length != 0)

    knn_sample.map(x => x._1 + "|" + x._2 + "|" + x._3 + "|" + x._4 + "|" + x._5 + "|" + x._6)
      //.repartition(16)
      .saveAsTextFile(outputPath + "/knn_sample")

    val sim_ques = sim_ques_all.filter(x => {
      (if (x._1.length > shortQuesLen) x._3 > similarity else x._3 > highSimilarity)
    })

    sim_ques.map(x => x._1 + "|" + x._2 + "|" + x._3 + "|" + x._4 + "|" + x._5 + "|" + x._6)
      .repartition(16)
      .saveAsTextFile(outputPath + "/similar_questions")

    knn_sample.map(x => (x._1, x._4)).distinct()
      .map(x => x._1 + "|" + x._2)
      .repartition(8).saveAsTextFile(recommQuesPath)
  }

    def run(params: Params): Unit = {

      val yesterdayUnans = params.yesterdayUnans
      val recentQuesPath = params.recentQuesPath
      val stopFilePath = params.stopFilePath
      val domainPath = params.domainPath
      val outputPath = params.outputPath
      val removeMode = params.removeMode
      val dfsUri = params.dfsUri
      val similarity = params.similarity
      val recommQuesPath = params.recommQuesPath
      val shortQuesLen = params.shortQuesLen
      val highSimilarity = params.highSimilarity
      val useKnn = params.useKnn
      val useLucene = params.useLucene


      HDFSUtil.removeOrBackup(removeMode, dfsUri , outputPath )
      HDFSUtil.removeOrBackup(removeMode, dfsUri , recommQuesPath )

      if(useKnn) {
        val conf = new SparkConf().setAppName("CalcSimQuestions")
        val sc = new SparkContext(conf)
        val domainList = Source.fromFile(domainPath).getLines().toList.map(_.toLowerCase()).filter(_.trim.length > 0)
        var map: Map[String, Int] = Map()
        domainList.foreach(domain => {
          map += (domain -> 3)
        })
        val domainMap = map
        val bc_domainList = sc.broadcast(domainList)

        //获取最近一月问句，（简洁问题、次数、知识点、已回答标志、问句分词）
        val recent_ques = getMonthQuestions(recentQuesPath, sc, stopFilePath, bc_domainList)

        //获取昨天未回答问句，（昨日问句，昨日频次，问句分词）
        val unans = getYesterdayUnans(yesterdayUnans, sc, stopFilePath, bc_domainList)
        val yest_unans = sc.broadcast(unans.collect().toList)

        //计算相似度,返回结果（昨天问句，最近问句，相似度，昨日频次，最近知识点，最近次数、最近已回答标志）
        val sim_ques_all = if(useLucene) {
          luceneCalculate(recent_ques, unans, shortQuesLen, highSimilarity, sc)
        } else {
          execCalculate(recent_ques, yest_unans, domainMap, shortQuesLen, highSimilarity)
        }

        //存储
        saveSimilarQues(sim_ques_all, outputPath, recommQuesPath, shortQuesLen, highSimilarity, similarity)
      }
  }

  /**
    * 计算两个句子测相似度
    * @param q1
    * @param q2
    * @param q1_seg
    * @param q2_seg
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
        .text("Unanswered questions path in yesterday")
        .action((x, c) => c.copy(yesterdayUnans = x))
      opt[String]('e', "recentQuesPath")
        .text("Path for recently questions info")
        .action((x, c) => c.copy(recentQuesPath = x))
      opt[String]('o', "output")
        .text("Output path for similarity questions")
        .action((x, c) => c.copy(outputPath = x))
      opt[String]("recommQuesPath")
        .text("Path for questions with recommend faq")
        .action((x, c) => c.copy(recommQuesPath = x))
      opt[String]("stopFilePath")
        .text("stop words file path")
        .action((x, c) => c.copy(stopFilePath = x))
      opt[String]("domainPath")
        .text("业务词路径")
        .action((x, c) => c.copy(domainPath = x))
      opt[Double]('s', "similarity")
        .text("greater than this similarity value will be viewed similar questions")
        .action((x, c) => c.copy(similarity = x))
      opt[String]('u', "dfsUri")
        .text("hadoop fs default name")
        .action((x, c) => c.copy(dfsUri = x))
      opt[Boolean]("removeMode")
        .text("remove or backup output dir that exists")
        .action((x, c) => c.copy(removeMode = x))
      opt[Int]("shortQuesLen")
        .text("Short unanswered questions length")
        .action((x, c) => c.copy(shortQuesLen = x))
      opt[Double]("highSim")
        .text("High similarity value for short unanswered questions")
        .action((x, c) => c.copy(highSimilarity = x))
      opt[Boolean]("useKnn")
        .text("Use knn or not")
        .action((x, c) => c.copy(useKnn = x))
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
                     yesterdayUnans: String = "hdfs://172.16.0.1:9000/production/nlp/output/unanswered_analysis/step_filter",
                     recentQuesPath: String = "hdfs://172.16.0.1:9000/production/nlp/output/unanswered_analysis/step_recently_questions",
                     stopFilePath: String = "/opt/xiaoi/txt/stopwords_least.txt",
                     domainPath: String = "domain_words.txt",
                     similarity: Double = 0.5,
                     shortQuesLen: Int = 5,
                     highSimilarity: Double = 0.5,
                     outputPath: String = "hdfs://172.16.0.1:9000/production/nlp/output/unanswered_analysis/step_sim_questions",
                     recommQuesPath: String = "hdfs://172.16.0.1:9000/production/nlp/output/unanswered_analysis/step_recomm_ques_list",
                     dfsUri: String = "",
                     useKnn: Boolean = false,
                     useLucene: Boolean = false,
                     luceneTopN: Int = 100,
                     removeMode: Boolean = true)
}

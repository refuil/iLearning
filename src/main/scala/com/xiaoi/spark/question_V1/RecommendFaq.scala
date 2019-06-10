package com.xiaoi.spark.question

import com.xiaoi.common.HadoopOpsUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2017/9/4 0004.
  */
object RecommendFaq {

  /**
    * 使用knn算法
    * 1.以未回答问题为key分组，按照相似度排序取topN，默认取11个
    * 2.未回答问题和标准问为key，计算频次，根据频次对未回答问题的标准问取topN，默认5个
    * 3.存储（未回答问题，标准问，次数）
    * @param params
    * @param sc
    */
  def getKnnRecommend(params: Params, sc: SparkContext):RDD[String] = {
    val inputPath = params.knnSampleInput
    val outputPath = params.knnRecommPath
    val k = params.k
    val nClasses = params.nClasses
    val useDebugMode = params.useDebugMode

    val knn_result = sc.textFile(inputPath).map(x => {
      val fields = x.split("\\|", -1)
      // 未回答问题、问题、相似度、未回答问题次数、标准问
      (fields(0), (fields(1), fields(2).toDouble, fields(3).toInt, fields(4)))
    }).groupByKey().mapValues(iter => {
      iter.toList.sortBy(-_._2).take(k)
    }).flatMapValues(x => x).map(x => ((x._1, x._2._4), 1))
      .reduceByKey(_ + _)
      .map(x => (x._1._1, (x._1._2, x._2)))
      .groupByKey().mapValues(iter => {
      iter.toList.sortBy(-_._2).take(nClasses)
    }).flatMapValues(x => x).map(x => (x._1, x._2._1, x._2._2 * 1.0)).map(x => x._1 + '|' + x._2 + '|' + x._3)

    if(useDebugMode){
      knn_result.saveAsTextFile(outputPath)
    }
    knn_result
  }

  /**
    * LR推荐结果拆分
    * @param line
    * @return
    */
  def parseLRMultiLabelsLine(line: String): List[(String, String, Double)] = {
    val unans_result = line.split("\\t", -1)
    val unans = unans_result(0)
    unans_result(2).split("\\|", -1).map(one_result => {
      val comma_idx = one_result.indexOf(',')
      val prob = one_result.substring(0, comma_idx).toDouble
      val faq = one_result.substring(comma_idx + 1)
      (unans, faq, prob)
    }).toList
  }

  /**
    * 合并knn和LR的推荐标准问结果
    * @param params
    * @param sc
    */
  def mergeKnnLr(params: Params, sc: SparkContext, knn_result: RDD[String]):Unit = {
    val lrRecommPath = params.lrRecommPath
    val nClasses = params.nClasses
    val faqIdPath = params.faqIdMapPath
    val outputPath = params.mergedRecommPath
    val yesterdayUnansPath = params.yesterdayUnansPath
    val filterFaq = params.filterFaq

    val yesterday_unans = sc.textFile(yesterdayUnansPath)
      .map(x => {
        val fields = x.split("\\|", -1)
        val ques = fields(0)
        val ori = fields(1)
        (ques, ori)
      })

    // 未回答问题、标准问、权重
    val knn = knn_result.map(x => {
      val fields = x.split("\\|", -1)
      (fields(0), fields(1), fields(2).toDouble)
    })

    val lr_1 = sc.textFile(lrRecommPath + "/model_LR_TF_IDF_large").flatMap(parseLRMultiLabelsLine)
    val lr_2 = sc.textFile(lrRecommPath + "/model_LR_TF_IDF_middle").flatMap(parseLRMultiLabelsLine)
    val lr_3 = sc.textFile(lrRecommPath + "/model_LR_TF_IDF_small").flatMap(parseLRMultiLabelsLine)

    // 未回答问题、推荐标准问、置信度、推荐问序号、推荐标准问个数
    val recomm = knn.union(lr_1).union(lr_2).union(lr_3)
      //未回答问题，标准问，概率
      .map(x => ((x._1, x._2), x._3))
      .reduceByKey(_ + _)
      //      .map(x=>(x._1._1, List[(String, Double)]((x._1._2, x._2))))
      .map(x => (x._1._1, List[(String, Double)]((x._1._2, x._2))))
      //      .map(x => (x._1._1, List[(String, Double)]((x._1._2, x._2))))
      .reduceByKey(_ ++ _)
      .mapValues(iter => {
        val n_recomm = iter.sortBy(-_._2).take(nClasses)
        val len = n_recomm.length
        val recomm = new ListBuffer[(String, Double, Int, Int)]
        for (i <- 0 to (len - 1)) {
          val (faq, confidence) = n_recomm(i)
          recomm += Tuple4(faq, confidence, i + 1, len)
        }
        recomm.toList
      })
      .flatMapValues(x => x)
      .map(x => (x._1, x._2._1, x._2._2, x._2._3, x._2._4))

    //和标准问相同的未回答问题
    val filterSet = recomm.map(x=>(x._1, x._2)).filter(x=>x._1.trim.equals(x._2.trim)).map(_._1).collect.toSet
    val bcFilterSet = if(filterFaq && filterSet.size > 0) sc.broadcast(filterSet) else null

    //过滤和标准问相同的未回答
    val filterRecomm = recomm.filter(x=> if(bcFilterSet != null ) !bcFilterSet.value.contains(x._1) else true).cache()

    filterRecomm.map(x => (x._1, x._5))
      .distinct
      .map(x => x._1 + "|" + x._2)
      .saveAsTextFile(outputPath + "/recommend_cnt")

    // 标准问、标准问ID
    val faq_id = sc.textFile(faqIdPath).map(x => {
      val fields = x.split("\\|", -1)
      (fields(0), fields(1))
    })

    // 未回答问题、标准问、标准问序号、标准问ID、置信度
    val ques_recomm_all = filterRecomm.map(x => (x._2, (x._1, x._3, x._4)))
      .leftOuterJoin(faq_id)
      .map(x => (x._2._1._1, x._1, x._2._1._3, x._2._2.getOrElse(""), x._2._1._2))
      .cache()

    ques_recomm_all.map(x => x._1 + "|" + x._2 + "|" + x._3 + "|" + x._4 + "|" + x._5)
      .saveAsTextFile(outputPath + "/all")

    // 未回答问题、标准问、标准问ID
    val ques_recomm_one = ques_recomm_all.filter(_._3 == 1)
      .map(x => (x._1, (x._2, x._4)))

    // 未回答问题、标准问、标准问ID、原始问题
    val ori_recomm = ques_recomm_one.join(yesterday_unans)
      .map(x => (x._1, x._2._1._1, x._2._1._2, x._2._2))
      .distinct()

    ori_recomm.map(x => x._1 + "|" + x._2 + "|" + x._3 + "|" + x._4).saveAsTextFile(outputPath + "/recommend_one")
  }

  //TODO 修改主表插入逻辑后，删除该段
  def getSimOriQues(params: Params, sc: SparkContext): Unit ={
    val unansRecommPath = params.unansRecommPath
    val simQuesPath = params.simQuesPath
    //    val outputPath = params.simOriQuesPath
    val yesterdayUnansPath = params.yesterdayUnansPath
    val recentlyQuesPath = params.unansCntInput
    val relatedCntOutput = params.relatedCntOutput

    // 昨日未回答问题 格式：未回答问题、标准问、标准问序号
    val unans_recomm = sc.textFile(unansRecommPath).map(x => x.split("\\|")(0)).distinct().collect()
    val bc_unans_recomm = sc.broadcast(unans_recomm)

    // 得到未回答问题的相似问题，格式：未回答问题、问题、相似度
    val flt_unans_sim = sc.textFile(simQuesPath).map(x => {
      val fields = x.split("\\|", -1)
      val ques = fields(0)
      val q = fields(1)
      val sim = fields(2).toDouble
      (ques, q, sim)
    }).distinct()
      .filter(x => bc_unans_recomm.value.contains(x._1))

    // 昨日未回答问题及其原始问题， 格式：简洁问题、原问题
    val yesterdayUnans = sc.textFile(yesterdayUnansPath).map(x => {
      val fields = x.split("\\|", -1)
      val ques = fields(0)
      val ori = fields(1)
      (ques, ori)
    })

    // 相似问题的简洁形式及其原始问题，格式：简洁问题、原问题
    val recentlyQues = sc.textFile(recentlyQuesPath).map(x => {
      val fields = x.split("\\|", -1)
      val ques = fields(0)
      val ori = fields(2)
      val answered = fields(4)
      (ques, (ori, answered))
    })

    // 未回答问题、原始问题、相似问原始问题、相似度、相似问是否已回答
    // 输出格式：未回答问题、相似问原始问题、相似度、相似问是否已回答
    val unans_sim = flt_unans_sim.map(x => (x._1, (x._2, x._3)))
      .join(yesterdayUnans)
      .map(x => (x._2._1._1, (x._1, x._2._2, x._2._1._2))) //相似问，（未回答，未回答原问题，相似度）
      .join(recentlyQues)
      .map(x => (x._2._1._1, x._2._1._2, x._2._2._1, x._2._1._3, x._2._2._2)) //未回答，未回答原问题，相似问原问题，相似度，回答标志
      .filter(x => x._2 != x._3).map(x => (x._1, x._3, x._4, x._5)) //未回答，相似问原问题，相似度，回答标志
      .distinct().cache()

    // 相似问题个数统计
    unans_sim.map(x => (x._1, 1))
      .reduceByKey(_ + _)
      .map(x => x._1 + "|" + x._2)
      .saveAsTextFile(relatedCntOutput)

    //    unans_sim.map(x => x._1 + "|" + x._2 + "|" + x._3 + "|" + x._4).saveAsTextFile(outputPath)
  }

  def run(params: Params): Unit = {
    val useKnn = params.useKnn
    val knnRecommPath = params.knnRecommPath
    val mergedRecommPath = params.mergedRecommPath
    val removeMode = params.removeMode
    val dfsUri = params.dfsUri

    val conf = new SparkConf().setAppName("RecommFaq")
    val sc = new SparkContext(conf)
    //删除输出路径
    HadoopOpsUtil.removeOrBackup(removeMode,dfsUri,knnRecommPath)
    HadoopOpsUtil.removeOrBackup(removeMode,dfsUri,mergedRecommPath)

    //计算KNN （未回答问题，标准问，次数）
    val knn_result = if(useKnn) getKnnRecommend(params, sc) else sc.makeRDD(List[String]())
    //合并LR和KNN
    mergeKnnLr(params, sc, knn_result)

    sc.stop()

  }

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("Get the recommend faq") {
      head("Get the recommend faq ")
      opt[String]("knnSampleInput")
        .text("Input data path for calc similarity questions")
        .action((x, c) => c.copy(knnSampleInput = x))
      opt[String]("knnRecommPath")
        .text("Output path for recommend faqs")
        .action((x, c) => c.copy(knnRecommPath = x))
      opt[Int]('k', "knn_k")
        .text("k in Knn algorithm")
        .action((x, c) => c.copy(k = x))
      opt[Int]('n', "n_classes")
        .text("n classes to remain")
        .action((x, c) => c.copy(nClasses = x))
      opt[Boolean]("useKnn")
        .text("Use knn or not")
        .action((x, c) => c.copy(useKnn = x))
      opt[Boolean]("useDebugMode")
        .text("If debug mode is true, save intermediate result")
        .action((x, c) => c.copy(useDebugMode = x))
      opt[String]("lrRecommPath")
        .text("lr-recommend path")
        .action((x, c) => c.copy(lrRecommPath = x))
      opt[String]("faqIdMapPath")
        .text("faq-id-mapping path")
        .action((x, c) => c.copy(faqIdMapPath = x))
      opt[String]("yesterdayUnans")
        .text("Path for unanswered questions in yesterday")
        .action((x, c) => c.copy(yesterdayUnansPath = x))
      opt[String]("mergedRecommPath")
        .text("Output path for unanswered recommend")
        .action((x, c) => c.copy(mergedRecommPath = x))
      opt[String]('u', "dfsUri")
        .text("hadoop fs default name")
        .action((x, c) => c.copy(dfsUri = x))
      opt[Boolean]('r', "removeMode")
        .text("remove or backup output dir that exists")
        .action((x, c) => c.copy(removeMode = x))
      opt[Boolean]("filterFaq")
        .text("Filter unanswer question that equals faq")
        .action((x, c) => c.copy(filterFaq = x))
      //TODO 以下参数待删除
      opt[String]("simQuesPath")
        .text("Unanswered similar questions path")
        .action((x, c) => c.copy(simQuesPath = x))
      //      opt[String]("simOriQuesPath")
      //        .text("Output path for filtered unanswered similar")
      //        .action((x, c) => c.copy(simOriQuesPath = x))
      opt[String]("unansRecommPath")
        .text("Unanswered-recommend path")
        .action((x, c) => c.copy(unansRecommPath = x))
      opt[String]("relatedCntOutput")
        .text("Output path for recommend faqs")
        .action((x, c) => c.copy(relatedCntOutput = x))
      opt[String]("unansCntInput")
        .text("Input path for unanswered questions with count in a period")
        .action((x, c) => c.copy(unansCntInput = x))
      checkConfig { params => success }
    }

    parser.parse(args, defaultParams).map {
      params => run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  case class Params(
                     //knn推荐
                     knnSampleInput: String = "",
                     k: Int = 11,
                     nClasses: Int = 5,
                     knnRecommPath: String = "",
                     dfsUri: String = "",
                     useKnn: Boolean = false,
                     useDebugMode: Boolean = false,
                     removeMode: Boolean = true,
                     //合并knn和lr
                     yesterdayUnansPath: String = "",
                     lrRecommPath: String = "",
                     faqIdMapPath: String = "",
                     mergedRecommPath: String = "",
                     filterFaq: Boolean = true,
                     //得到相似问的原问题，计算相似问数量，待删除变量
                     unansCntInput: String = "",
                     unansRecommPath: String = "",
                     simQuesPath: String = "",
                     //                     simOriQuesPath: String = "",
                     relatedCntOutput: String = ""
                   )
}
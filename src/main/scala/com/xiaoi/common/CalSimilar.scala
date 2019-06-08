package com.xiaoi.common

/**
  * created by yang.bai@xiaoi.com 2015-08-03
  * 编辑距离
  * spark-submit --master spark://davinci08:7077 \
  *              --executor-memory 10g \
  *              --class com.xiaoi.spark.CalSimilar \
  *              /opt/xiaoi/baiy/build_jar/cluster.jar
  */
object CalSimilar {

  /**
    * 计算编辑距离
    * @param str1 待计算的字符串1
    * @param str2 待计算的字符串2
    */
  def calEditSimilar(str1 : String, str2 : String) : Double = {
    val factor : Double =
      if (str1.length >= 8 || str2.length >= 8) {
        val ratio = str1.length.toDouble / str2.length.toDouble
        if (ratio > 2.0 || ratio < 0.5) 0.5
        else 1.0
      } else 1.0
    EditDistance.sim(str1, str2).toDouble * factor
  }

  /**
    * 计算jaccard
    * @param segment1 待计算的分词字符串1
    * @param segment2 待计算的分词字符串2
    */
  def calJaccardSimilar(segment1 : String, segment2 : String, separator : String) : Double = {
    val seg1 = segment1.split(separator)
    val seg2 = segment2.split(separator)
    val inSeg = seg1 intersect seg2
    val inLength = inSeg.length
    val unionLength = seg1.length + seg2.length - inLength
    inLength.toDouble / unionLength.toDouble
  }

  /**
    * 计算编辑距离，将相同英文词替换为一个字符，降低英文词权重
    * @param str1 待计算的字符串1
    * @param str2 待计算的字符串2
    */
  def calEditSimilarNew(str1: String, str2: String): Double = {
    val factor: Double =
      if (str1.length >= 8 || str2.length >= 8) {
        val ratio = str1.length.toDouble / str2.length.toDouble
        if (ratio > 2.0 || ratio < 0.5) 0.5
        else 1.0
      } else 1.0
    //如果字母相等，替换为一个字母A
    val parttern = "\\w+".r //字母数字混合的关键字
    val reg1 = parttern.findFirstIn(str1.replace(" ","")).getOrElse("")
    val reg2 = parttern.findFirstIn(str2.replace(" ","")).getOrElse("")
    if(!"".equals(reg1) && reg1.equals(reg2)){
      val rep1 = str1.replace(reg1,"a")
      val rep2 = str2.replace(reg2,"a")
      EditDistance.sim(rep1, rep2).toDouble * factor
    }else{
      EditDistance.sim(str1, str2).toDouble * factor
    }
  }

  /**
    * Calculate Jaccard with domain words
    * 使用业务词计算杰卡德
    * @param segment1 待计算的分词字符串1
    * @param segment2 待计算的分词字符串2
    * @param domainMap 业务关键词
    * @param separator 分词的分割符
    * @return
    */
  def domainJaccardSimilar(segment1: String, segment2: String, domainMap: Map[String,Int] ,separator: String=" " ): Double = {
    val seg1 = segment1.split(separator)
    val seg2 = segment2.split(separator)
    val inSeg = (seg1 intersect seg2).distinct
    val unionSeg = (seg1 union seg2).distinct
    val inSegValues = inSeg.map(domainMap.getOrElse(_,1))
    val unionSegValues = unionSeg.map(domainMap.getOrElse(_,1))
    val inSumValue = inSegValues.sum
    val unionSumValue = unionSegValues.sum
    val res = inSumValue.toDouble / unionSumValue.toDouble
    res
  }

  /**
    * 测试用
    * @param args
    */
  def main(args: Array[String]) {
    //    val conf = new SparkConf().setAppName("100000 records compute edit distance")
    //    val sc = new SparkContext(conf)
    //    val input_path = "hdfs://172.16.0.34:9000/experiment/baiy/output/oml_cluster/step_filter"
    //    val output_path = "hdfs://172.16.0.34:9000/experiment/baiy/output/step_edit"
    //    val stopFilePath = "/opt/xiaoi/txt/stopwords_least.txt"
    //
    //    val text = sc.textFile(input_path)
    //      .map(x => {
    //        val line = x.split("\t")
    //        (line(0), line(1), Segment.segment(line(0), " ", true, stopFilePath, false, ""))
    //      })
    //      .zipWithIndex()
    //      .map(x => (x._2, x._1))
    //      .cache()
    //    val edit_sim =  text.cartesian(text)
    //      .map(x => ((x._1._1, x._2._1), CalSimilar.calEditSimilar(x._1._2._2.replace(" ",""), x._2._2._2.replace(" ",""))))
    //      .filter(x => x._1._1 < x._1._2)
    //      .saveAsTextFile(output_path)
    //
    //    sc.stop()
    val str1 = "27             联想启天 A8150-N000扬声器不响，耳机可以"
    val str2 = "小乐              请您描述在什么情况下电脑"
    println(calEditSimilar(str1.replace(" ",""), str2.replace(" ","")))
    println(str1.length)
    println(str2.length)

  }
}

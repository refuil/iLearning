package com.xiaoi.common

/**
  * created by yang.bai@xiaoi.com 2015-07-31
  * 使用ansj做中文分词
  * spark-submit --master spark://davinci08:7077 \
  * --executor-memory 10g \
  * --class com.xiaoi.spark.Segment \
  * --jars /opt/spark/lib/ansj_seg-2.0.8.jar, \
  * /opt/spark/lib/nlp-lang-0.3.jar \
  * /opt/xiaoi/baiy/build_jar/cluster.jar
  * todo: 与jieba分词比较
  */

import scala.io.Source

object Segment {

  var initd = false

  val filter = new FilterRecognition()

  def init(stopWordsEnable: Boolean, stopWordsFile: String,
           stopNaturesEnable: Boolean, stopNaturesFile: String): Unit = {
    if (initd) return
    if (stopWordsEnable) {
      for (line <- Source.fromFile(stopWordsFile).getLines())
        filter.insertStopWord(line.trim)
    }
    if (stopNaturesEnable) {
      for (line <- Source.fromFile(stopNaturesFile).getLines())
        filter.insertStopNatures(line)
    }
    initd = true
  }

  /**
    * 使用自定义词典
    * @param words 词典列表
    */
  def loadWords(words: List[String]): Unit = {
    var forest = UserDefineLibrary.FOREST
    words.map(word => {
      val paramers = List(word, "userDefine", "1000").mkString("\t")
      val value = new Value(paramers)
      Library.insertWord(forest, value)
    })
  }

  /**
    * 中文分词：词典优先级最高
    * @param input
    * @param separator
    * @return
    */
  def dicSegment(input: String, separator: String): String  = {
    val terms = DicAnalysis.parse(input).recognition(filter)
    terms.map(_.getName).mkString(separator)
  }

  /**
    * 中文分词：词典优先级最高 结果带词性
    * @param input 待分词的句子
    * @param wordSeparator 分词结果的分隔符
    * @param posSeparator 词与词性的分隔符
    * @return
    */
  def dicSegmentWithPos(input: String, wordSeparator: String, posSeparator: String): String = {
    val terms = DicAnalysis.parse(input).recognition(filter)
    terms.map(x => x.getName + posSeparator + x.getNatureStr).mkString(wordSeparator)
  }

  /**
    * 中文分词：结果带词性
    * @param input 待分词的句子
    * @param wordSeparator 分词结果的分隔符
    * @param posSeparator 词与词性的分隔符
    * @return
    */
  def nlpSegmentWithPos(input: String, wordSeparator: String, posSeparator: String): String = {
    val terms = NlpAnalysis.parse(input).recognition(filter)
    terms.map(x => x.getName + posSeparator + x.getNatureStr).mkString(wordSeparator)
  }

  /**
    * 中文分词：精准分词
    * @param input
    * @param separator
    * @return
    */
  def accSegment(input: String, separator: String): String  = {
    val terms = ToAnalysis.parse(input).recognition(filter)
    terms.map(_.getName).mkString(separator)
  }

  /**
    * 中文分词：精准分词，包含词性
    * @param input
    * @param wordSeparator
    * @return
    */
  def accSegmentNature(input: String, wordSeparator: String, posSeparator: String): String  = {
    val terms = ToAnalysis.parse(input).recognition(filter)
    terms.map(x => x.getName + posSeparator + x.getNatureStr).mkString(wordSeparator)
  }

  /**
    * 中文分词，空格作为分词结果的分隔符
    * example :
    * sc.textFile(input_path).map(Segment.segment)
    * @param input 待分词的句子
    */
  def segment(input: String): String = {
    segment(input, " ")
  }

  /**
    * 中文分词，自定义分隔符
    * example :
    * sc.textFile(input_path).map(Segment.segment(_, " \\"))
    * @param input 待分词的句子
    * @param separator 分词结果的分隔符
    */
  def segment(input: String, separator: String): String = {
    val terms = NlpAnalysis.parse(input).recognition(filter)
    terms.map(_.getName).mkString(separator)
  }


  /**
    * 中文分词，返回指定词性的词
    *
    * @param input
    * @param specNatures
    * @return
    */
  def segmentWithSpecNature(input: String, specNatures: List[String]): List[String] = {
    filter.insertStopNatures(specNatures:_*)
    val terms = NlpAnalysis.parse(input).recognition(filter)
    terms.map(_.getName).toList
  }

  /**
    * Jieba Java Segment
    * @param input
    * @param separator
    * @return
    */
  def jieba_analysis(input: String, separator: String): String ={
    val stopWords = ConfigurationManager.getString(Constants.STOP_WORDS).split("\\|", -1)
    val segment = new JiebaSegmenter
    val seg_list = segment.sentenceProcess(input)
//    println(seg_list(0))
    if (stopWords != null) {
      for(sw <- stopWords){
        if(seg_list.contains(sw)) seg_list.remove(sw)
      }
    }else print("Stopwords is null")
    seg_list.mkString(separator)
  }

  /**
    * 中文分词 Util
    * example:
      val stop_words = Config.STOP_WORDS.split("\\|", -1).toArray
      val str = """据新华社电 中共中央总书记、国家主席、中央军委主席习近平12日上午在第十八届中央纪委第六次全会上发表重要讲话。
                 他强调，坚决遏制腐败现象滋生蔓延势头。惩治腐败这一手必须紧抓不放、利剑高悬，坚持无禁区、全覆盖、零容忍。"""
      var MySegment = new SegmentUtil(stopWords)
      val ret = MySegment.segment(str)
      println(ret)
    * @param stopWords
    * @param separator
    * @param natureSeparator
    * @param stopNatures
    */
  class SegmentUtil (
                      stopWords: Array[String] = null,
                      separator: String = " ",
                      natureSeparator: String = ":",
                      stopNatures: Array[String] = null) extends Serializable{

    val filter = new FilterRecognition
    //初始化停用词、停用词性等操作
    if (stopWords != null) {
      val sws = new java.util.ArrayList[String]
      for (item <- stopWords) sws.add(item)
      filter.insertStopWords(sws)
    }
    if (stopNatures != null) {
      for (line <- stopNatures)
        filter.insertStopNatures(line)
    }

    /**
      * @param input 待分词的句子
      */
    def segment(input: String): Seq[String] = {
      val splited = NlpAnalysis.parse(input).recognition(filter)

      val word = if (stopNatures != null) {
        for (i <- Range(0, splited.size()))
          yield splited.get(i).getName + natureSeparator + splited.get(i).getNatureStr
      } else for (i <- Range(0, splited.size())) yield splited.get(i).getName
      //    word.mkString(separator)
      word
    }

  }

  def main(args: Array[String]) {
    //    val path = "/develop/workspace/corpus/lenovo_keys_v2"
    val str = "一直手机上的，今天在电脑上儿都输错密码了？现在手机上也被锁了，怎么办？"
    Segment.init(true, "/opt/xiaoi/txt/stopwords_least.txt", false, "")
    //    Segment.loadWords(Source.fromFile(path).getLines().toList)
    //    println(Segment.dicSegment(str, " "))
    println(Segment.nlpSegmentWithPos(str, " ", ":"))
    //    println(Segment.accSegment(str, " "))
    println(Segment.accSegmentNature(str, " ", ":"))

        println(ToAnalysis.parse(str))
        println(NlpAnalysis.parse(str))
        println(DicAnalysis.parse(str))

    val s = Segment.accSegmentNature(str, " ", ":").split(" ")
      .filter(y => !y.endsWith(":m") && !y.endsWith(":s"))
      .map(y => println(y.split(":")(0)))

    val str1 = "thinkpad e450c笔记本安装不了msxml"
    println(Segment.segment("thinkpad e450c笔记本安装不了msxml", " "))

    println(Segment.segment(str1, " "))
    println(Segment.nlpSegmentWithPos(str1, " ", ":"))
    println(Segment.accSegmentNature(str1, " ", ":"))

    println(jieba_analysis(str1, " "))

  }

}

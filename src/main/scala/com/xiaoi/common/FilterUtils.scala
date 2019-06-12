package com.xiaoi.common

import org.apache.spark.{SparkConf, SparkContext}

/**
 * created by yang.bai@xiaoi.com 2015-07-30
 * 文本过滤工具
 */
object FilterUtils {

  /**
   * 中文字符出现次数占比是否超过阈值
   * @param input 待过滤字符串
   * @param threshold 阈值
   */
  def chineseRatioCheck(input : String, threshold : Double) : Boolean = {
    chineseRatio(input) >= threshold
  }

  /**
   * 中文字符出现次数占比
   * @param input 待计算中文占比的字符串
   */
  def chineseRatio(input : String) : Double = {
    val chineseTime : Double = input.toCharArray
      .filter(isChineseLikeChar)
      .length
      .toDouble
    chineseTime / input.length
  }

  /**
   * 字符是否为中文
   * @param input 待检查的字符
   */
  def isChineseLikeChar(input : Char) : Boolean = {
    if ((input >= '0' && input <= '9') ||
      (input >= 'a' && input <= 'z') ||
      (input >= 'A' && input <= 'Z')) {
      false
    } else if (Character.isLetter(input)) {
      true
    } else {
      false
    }
  }

  /**
   * 非法字符出现次数(不一定连续)超过阈值
   * @param input 待检查的字符串
   * @param illegalTimes 非法字符次数阈值
   */
  def illegalCharCheck(input : String, illegalTimes : Int) : Boolean = {
    val symbols = """.*[≯@Ф〇Ω×÷Π？！⊙【】☆□凵€╯╰：〈〔★…▽ˊψ━━┓艹↖∞Ҿ♥◑▽￢✘《》“”‘’。].*"""
    var regex = ""
    for (i <- 0 to illegalTimes - 1) {
      regex += symbols
    }
    val r = ("^" + regex + ".*$").r
    input match {
      case r(n) => false
      case _ => true
    }
  }

  /**
   * 去字符串头尾的符号
   * @param input 待处理的字符串
   */
  def removeHeadAndTailSymbol(input : String) : String = {
    val headRegex = "^[^\\u4e00-\\u9fa5a-zA-Z0123456789]{1,}"
    val tailRegex = "[^\\u4e00-\\u9fa5a-zA-Z0123456789]{1,}$"
    input.replaceFirst(headRegex, "").replaceFirst(tailRegex, "")
  }

  /**
    * 去除标点符号（中文、字母、数字和#$@*四个字符之外的字符）
    * @param input
    * @return
    */
  def removePunctuates(input: String) : String = {
    val regex = "[^\\u4e00-\\u9fa5a-zA-Z0-9#$@*]+$";
    input.replaceAll(regex, "")
  }

  /**
    * 去除中文标点符号（句号、分号、逗号、冒号、双引号、小括号、顿号、问号、书名号、中括号、叹号）
    * 并且将两个及以上的空格替换为一个
    * @param input
    * @return
    */
  def removeChinesePunctuates(input: String) : String = {
    // 该表达式可以识别出： 。 ；  ， ： “ ”（ ） 、 ？ 《 》【 】 ！这些标点符号。
    val cpRegex = "[\\u3002\\uff1b\\uff0c\\uff1a\\u201c\\u201d\\uff08\\uff09\\u3001\\uff1f\\u300a\\u300b\\u3010\\u3011\\uff01]"
    val spRegex = "\\s{2,}"
    input.replaceAll(cpRegex, " ").replaceAll(spRegex, " ")
  }

  /**
   * 字符串中的符号改为空格，2个以上到空格改为1个
   * @param input 待处理的字符串
   * @return
   */
  def replaceSymbolToSpace(input : String) : String = {
    val symbolRegex = "[^\\u4e00-\\u9fa5a-zA-Z0123456789]{1,}"
    val removeSymbol = input.replaceAll(symbolRegex, " ").trim
    val spaceRegex = "\\s{2,}"
    removeSymbol.replaceAll(spaceRegex, " ")
  }

  /**
   * 有选择的去除 中括号内的内容
   * @param input 待处理的字符串
   * @return
   */
  def removeBracket(input : String) : String = {
    input
      .replaceAll("\\[\\{.*\\]", "")
      .replaceAll("\\[\\(.*\\]", "")
      .replaceAll("\\[:.*\\]", "")
      .replaceAll("\\[\\^.*\\]", "")
      .replaceAll("\\[[0-9].*\\]", "")
      .replaceAll("\\[@.*\\]", "")
      .replaceAll("\\[[iI]mage.*\\]", "")
      .replaceAll("\\[[fF]ace.*\\]", "")
  }

  /**
   * 去字符串中的全角字符
   * @param input 待处理的字符串
   * @return
   */
  def removeSBC(input : String) : String = {
    val sbcRegex = "[\\uFF00-\\uFFFF]"
    input.replaceAll(sbcRegex, "")
  }

  /**
    * 是否仅仅包含重复字符，比方aaa，啊啊啊，哈哈哈哈这样的
    * 最小重复个数通过num制定
    * @param input
    * @param num
    * @return
    */
  def onlyDuplicateCharCheck(input: String, num: Int): Boolean = {
    val regex = ("^(.*)\\1{" + (num - 1) + ",}$").r
    input match {
      case regex(_*) => false
      case _ => true
    }
  }

  /**
    * 是否不包含超过指定长度的重复字符（中文、字母）
    * @param input
    * @param num
    * @return
    */
  def notContDuplicateCharCheck(input: String, num: Int): Boolean = {
    val regex = ("([\\u4e00-\\u9fa5a-zA-Z])\\1{" + (num - 1) + ",}").r
    (regex findFirstIn input) == None
  }

  /**
    * 去除声音模式
    * 1.声音=***.amr
    * @param input
    * @return
    */
  def removeVoiceQues(input: String): String = {
    val regex = (".*声音=.*amr.*").r
    regex.replaceAllIn(input, "")
  }

  /**
    * 是否包含声音模式
    * 1.声音=***.amr
    * @param input
    * @return
    */
  def isVoiceQues(input: String): Boolean = {
    val regex = (".*声音=.*amr.*").r
    regex.findFirstIn(input).isDefined
  }

  /**
    * 去除图片链接
    * 1.图片[10位以上字母数字]
    * 2.Image【：|=】{10位以上字母数字}+图片扩展名
    * @param input
    * @return
    */
  def removePicLike(input: String): String = {
    val regex = (".*图片.*[0-9A-Za-z\\s]{10,}.*|" +
      "\\[(?i)Image:\\{[A-Za-z0-9\\-]{10,}\\}\\.gif\\]|" +
      "\\[(?i)Image:\\{[A-Za-z0-9\\-]{10,}\\}\\.jpg\\]|" +
      "\\[(?i)Image:\\{[A-Za-z0-9\\-]{10,}\\}\\.png\\]|" +
      "\\[(?i)Image=\\{[A-Za-z0-9\\-]{10,}\\}\\.gif\\]|" +
      "\\[(?i)Image=\\{[A-Za-z0-9\\-]{10,}\\}\\.jpg\\]|" +
      "\\[(?i)Image=\\{[A-Za-z0-9\\-]{10,}\\}\\.png\\]").r
    regex.replaceAllIn(input, "")
  }

  /**
    * 是否包含图片链接
    * @param input
    * @return
    */
  def isPicQues(input: String): Boolean = {
    val regex = (".*图片.*[0-9A-Za-z\\s]{10,}.*|" +
      "\\[(?i)Image:\\{[A-Za-z0-9\\-]{10,}\\}\\.gif\\]|" +
      "\\[(?i)Image:\\{[A-Za-z0-9\\-]{10,}\\}\\.jpg\\]|" +
      "\\[(?i)Image:\\{[A-Za-z0-9\\-]{10,}\\}\\.png\\]|" +
      "\\[(?i)Image=\\{[A-Za-z0-9\\-]{10,}\\}\\.gif\\]|" +
      "\\[(?i)Image=\\{[A-Za-z0-9\\-]{10,}\\}\\.jpg\\]|" +
      "\\[(?i)Image=\\{[A-Za-z0-9\\-]{10,}\\}\\.png\\]").r
    regex.findFirstIn(input).isDefined
  }

  /**
    * 是否包含中文字符
    * @param input
    * @return
    */
  def containChineseChar(input: String): Boolean = {
    val regex = "[\\u4e00-\\u9fa5]".r
    regex.findFirstIn(input).isDefined
  }

  /**
    * 去除QQ标记
    * 1.@QQ号码
    * 2.@QQ昵称(QQ号码)
    * 3.括号包围起来的@QQ号码
    * @param input
    * @return
    */
  def removeQQLike(input: String):String = {
    val regex = ("@.*\\(\\d{5,12}\\)|" + "\\[CQ:at,qq=\\d{5,13}\\]|" +
      "\\(@[0-9]{5,12}\\)|" +
      "\\[@[0-9]{5,12}\\]|" +
      "@[0-9]{5,12}\\]|" +
      "\\[@[0-9]{5,12}|" +
      "@[0-9]{5,12}\\)|" +
      "\\(@[0-9]{5,12}|" +
      "@[0-9]{5,12}").r
    regex.replaceAllIn(input, "")
  }

  /**
    * 去除表情符号
    * 1.特定开始结束符号+中文：&#91；大笑&#93；
    * 2.[CQ:face,id=长度在1-4之间的数字]
    * 3.[face+1-4位数字.gif]
    * 4.[face+1-3位数字]
    * 5.原创表情+10位以上的字母数字空格
    * 6.[1-3位中文]重复2次以上:[悲伤][悲伤]
    * @param input
    * @return
    */
  def removeFaceLike(input: String): String = {
    val regex = ("&#91;[\\u4e00-\\u9fa5]{1,3}&#93;|" +
      "\\[CQ:face,id=\\d{1,4}\\]|" +
      "\\[?(?i)face\\d{1,4}\\.gif\\]?|" +
      "\\[?(?i)Face:\\d{1,3}\\]?|" +
      "原创表情.*[A-Za-z\\d\\s]{10,}|" +
      "(\\[[\\u4e00-\\u9fa5]{1,3}\\])\\1{1,}").r
    regex.replaceAllIn(input, "")
  }

  /**
    * 去除16进制字符串
    * [8位以上的字母数字空格]
    * @param input
    * @return
    */
  def removeHexStr(input: String): String = {
    val regex = "\\[[A-Za-z0-9\\s]{8,}\\]".r
    regex.replaceAllIn(input, "")
  }

  /**
    * 去除特定字符外的其他字符
    * 留下的字符包括:中文、字母、数字、#$@*
    * @param input
    * @return
    */
  def removeAbnormalChar(input: String): String = {
    val regex = "[^\\u4e00-\\u9fa5A-Za-z0-9#$@*]".r
    regex.replaceAllIn(input, "")
  }

  /**
    * 去除无意义字符串
    * 1.请您一次只问一个问题，而且尽量简短哟!以及各种短板
    * @param input
    * @return
    */
  def removeMeaninglessWords(input: String): String = {
    //    val regex = ("请您一次只问一个问题，而且尽量简短哟！").r
    val regex = ("请您一次只问一个问题，而且尽量简短哟！|" +
      "请您一次只问一个问题，而且尽量简短哟|" +
      "请您一次只问一个问题，而且尽量简|" +
      "请您一次只问一个问题，而且尽|" +
      "请您一次只问一个问题，").r
    regex.replaceAllIn(input, "")
  }


  /**
    * 去掉问句中的回车换行
    * @param input
    * @return
    */
  def removeCFLR(input: String): String = {
    input.replaceAll("\\r\\n", "").replaceAll("\\n", "").replaceAll("\\r", "")
  }

  /**
    * 过滤掉聊天问句
    * @param ques
    * @return
    */
  def removeExcludeQues(ques: String): Boolean={
    //过滤问句,匹配到的faq
    val excludeMatch = Seq("^\\\\d+$", ".*[你您]好.*", "亲.*", ".*[Hh]i.*", ".*我知道了.*", ".*你是谁.*",
      ".*谢谢.*", ".*在吗.*", ".*这样啊.*", ".*骂人.*", ".*请教个问题.*", ".*哈哈.*", ".*再见.*",
      ".*蠢货.*", ".*你看了吗.*", ".*答非所问.*", ".*回话.*", ".*怎么用你.*", ".*问什么.*", ".*你服务不错.*",
      ".*你个猪头.*", ".*神经病.*", ".*你个小三.*", ".*讨人喜欢.*", ".*长得好看.*", ".*忙么.*", ".*我饿了.*",
      ".*客气.*", ".*[早晚]上好.*", ".*谈.*对象.*", ".*结婚.*", ".*不理.*", ".*泥马.*", ".*妈逼.*", ".*你妹.*",
      ".*你大爷.*", ".*滚蛋.*", ".*去你妈.*", ".*草你.*", ".*操你.*", "尼玛.*")
    !excludeMatch.exists(e => ques.matches(e))
  }

  /**
    * 过滤特定的标准问
    * @param faq
    * @return
    */
  def removeExcludeFaq(faq: String): Boolean= {
    //去除特定的语句
    val excludeFaqs = Seq("查看提问技巧", "电脑反问", "用户描述不清楚或机器人未理解", "机器人提供的方案无效果，未解决我的问题")
    !excludeFaqs.exists(x => faq.equals(x))
  }

  /**
    * 删除文本中的网址信息/URL
    * @param ques
    */
  def removeURL(ques: String): String ={
    val URLSTR = "((http|ftp|https)://)(([a-zA-Z0-9._-]+.[a-zA-Z]{2,6})|" +
      "([0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}))(:[0-9]{1,4})*(/[a-zA-Z0-9&%_./-~-]*)?"
    ques.replaceAll(URLSTR, "")
  }

  /**
   * test
   */
  def main(args : Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("filter question")
    val sc = new SparkContext(conf)
    //    val input_path = "hdfs://172.16.0.34:9000/experiment/italk/data2015/ask"
    //    val output_path = "hdfs://172.16.0.34:9000/experiment/baiy/output/step_filter"
    val input_path = "/opt/ask/2017/06/06/*"
    val output_path = "/opt/ask/step_filter"
    sc.textFile(input_path)
      .map(_.split("\\|"))
      .filter(x => x.length >= 16
                && x(15).length >= 3
                && x(15).substring(0, 3).equals("nul")
                && x(6).equals("0"))
      .map(x => (x(3), removeHeadAndTailSymbol(x(3))))
      .distinct()
      .filter(x => x._2.length > 2
                && chineseRatioCheck(x._2, 0.8))
      .zipWithIndex()
      .map(x => x._2.toString + "|" + x._1._1 + "|" + x._1._2)
      .saveAsTextFile(output_path)
    sc.stop()
  }

}

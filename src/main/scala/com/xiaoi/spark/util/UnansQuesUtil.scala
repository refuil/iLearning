package com.xiaoi.spark.util

import com.xiaoi.common.FilterUtils

/**
 * UnansQuesUtil
 *
 * @author Simon Lee
 * @version 15/12/3
 */
object UnansQuesUtil {

  /**
   * 将问题进行简化，主要执行以下简化
   * 1.去除无用字符：请您一次只问一个问题等
   * 2.去除16进制字符串
   * 3.去除声音的片段：.*声音=.*amr.*
   * 4.去除图片地址：图片，Image .jpg等
   * 5.去除QQ号：@qq号
   * 6.去除表情符号：Face，原创表情+数字等
   * 7.去除中文标点符号
   * @param str 问题
   * @return 简化后的问题
   */
  def quesSimplify(str: String): String = {
    val ques_meaning = FilterUtils.removeMeaninglessWords(str)
    val ques_f = basicSimplify(ques_meaning)
    FilterUtils.removeChinesePunctuates(ques_f).trim.toLowerCase()
  }

  /**
   * 将已忽略的问题进行简化，用来对问题进行过滤
   * 1.进行 基本的简化处理
   * 2.去除首尾标点
   * 3.去除中文、字母、数字、#$@*之外的字符
   * @param str 问题
   * @return 简化后的问题
   */
  def ignoreQuesSimplify(str: String): String = {
    val ques_hex = basicSimplify(str)
    val ques_f = FilterUtils.removeHeadAndTailSymbol(ques_hex)
    FilterUtils.removeAbnormalChar(ques_f).trim.toLowerCase()
  }

  /**
   * 检查简化后的问题是否是要处理的未回答问题
   * 1.ex字段是否为空
   * 2.长度是否在合理的范围之内
   * 3.是否包含中文
   * 4.是否包含超过数量的非法字符
   * 5.是否包含超过长度的重复文字
   * 6.不包含声音模式
   * 7.不是图片模式
   * @param ques
   * @param ex
   * @param minQLen
   * @param maxQLen
   * @param illegalCharNum
   * @param dupCharNum
   * @param onlyChinese
   * @return
   */
  def quesCheck(ques: String, ex: String, minQLen: Int = 0, maxQLen: Int = 60,
                illegalCharNum: Int = 4, dupCharNum: Int = 4, onlyChinese: Boolean = true): Boolean = {
    val exValid = ex == null || ex.toLowerCase == "null" || ex.trim.length == 0
    val lengthValid = ques.length >= minQLen && ques.length <= maxQLen
    val containsZh = FilterUtils.containChineseChar(ques)
    val illegalCharChk = FilterUtils.illegalCharCheck(ques, illegalCharNum)
    val dupCheck = FilterUtils.notContDuplicateCharCheck(ques, dupCharNum)
    val notVoice = !FilterUtils.isVoiceQues(ques)
    val notPic = !FilterUtils.isPicQues(ques)
    exValid && lengthValid && containsZh && illegalCharChk && dupCheck && notVoice && notPic
  }

  /**
   * 功能同上，只是不检查回答类型
   * @param ques
   * @param ex
   * @param minQLen
   * @param maxQLen
   * @param illegalCharNum
   * @param dupCharNum
   * @param onlyChinese
   * @return
   */
  def normalCheck(ques: String, ex: String, minQLen: Int = 0, maxQLen: Int = 60,
                   illegalCharNum: Int = 4, dupCharNum: Int = 4, onlyChinese: Boolean = true): Boolean = {
    val exValid = ex == null || ex.toLowerCase == "null" || ex.trim.length == 0
    val lengthValid = ques.length >= minQLen && ques.length <= maxQLen
    val containsZh = FilterUtils.containChineseChar(ques)
    val illegalCharChk = FilterUtils.illegalCharCheck(ques, illegalCharNum)
    val dupCheck = FilterUtils.notContDuplicateCharCheck(ques, dupCharNum)
    val notVoice = !FilterUtils.isVoiceQues(ques)
    val notPic = !FilterUtils.isPicQues(ques)
    exValid && lengthValid && containsZh && illegalCharChk && dupCheck && notVoice && notPic
  }


  /**
   * 对问题进行基本的简化处理
   * 1.去除16进制字符串
   * 2.去除声音的片段：.*声音=.*amr.*
   * 3.去除图片地址：图片，Image .jpg等
   * 4.去除QQ号：@qq号
   * 5.去除表情符号：Face，原创表情+数字等
    * 6.去除\n,\r
   * @param str 问题
   * @return 简化后的问题
   */
  def basicSimplify(str: String): String = {
    val ques_voice = FilterUtils.removeVoiceQues(str)
    val ques_pic = FilterUtils.removePicLike(ques_voice)
    val ques_qq = FilterUtils.removeQQLike(ques_pic)
    val ques_face = FilterUtils.removeFaceLike(ques_qq)
    val ques_hex = FilterUtils.removeHexStr(ques_face)
//    ques_hex
    FilterUtils.removeCFLR(ques_hex)
  }

}

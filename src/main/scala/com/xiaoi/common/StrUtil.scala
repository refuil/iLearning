package com.xiaoi.common

import scala.collection.mutable.ListBuffer

object StrUtil {

  //去除字符串中转义字符:[\,"]
  def esc(str: String): String = {
    str.replaceAll("\\\\", "\\\\\\\\").replaceAll("\"", "\\\\\"")
  }

  //去除特殊字符
  def removeChar(raw: String, regex: String ): String = {
    val Pattern = s""".*([^\\pP${regex}]+).*""".r
    raw match {
      case Pattern(c) => raw.replaceAll(s"""[\\pP${regex}]+""", " ").trim
      case _ => raw
    }
  }

  //判定是否为纯非法符号组成
  def isIllegalChar(raw: String, regex: String): Boolean = {
    val Pattern = s""".*([^\\pP${regex}]+).*""".r
    raw match {
      case Pattern(c) => false
      case _ => true
    }
  }

  //contain chinese
  def containChinese(raw: String): Boolean = {
    val Pattern = s""".*([\u4e00-\u9fa5]).*""".r
    raw match {
      case Pattern(c) => true
      case _ => false
    }
  }

  /**
    * 生成数据库主键ID
    */
  def uuid() = java.util.UUID.randomUUID().toString.replace("-", "")

  /**
    * 去除最前和最后的括号
    */
  def removeBrackets(str:String):String = {
    var rep_str = str
    if(rep_str.indexOf("(")==0) rep_str = rep_str.substring(1,rep_str.length())
    if(rep_str.lastIndexOf(")")==(rep_str.length()-1)) rep_str = rep_str.substring(0,rep_str.length()-1)
    rep_str
  }

  /**
    * 去除最前和最后的双引号
    */
  def removeQuotations(s:String):String = {
    val pattern1 = "\"(.*)\"".r
    val pattern2 = "(.*)\"".r
    val pattern3 = "\"(.*)".r
    s match {
      case pattern1(a) => a
      case pattern2(a) => a
      case pattern3(a) => a
      case _ => s
    }
  }

  /**
    * 判断是否是常用十进制的小数，整数 ，支持(2.1， .1 ，2. ，100)
    */
  def isDecimalNum(s:String):Boolean = {
    val pattern = "[0-9]+(.[0-9]*)?|.{1}[0-9]+".r
    s match {
      case pattern(_) => true
      case _ => false
    }
  }

  /**
    * 任意元组转换成以指定间隔拼接的字符串
    * @param tuple
    * @param split
    * @return
    */
  def tupleToString(tuple: Product, split: String): String ={
    var line = ListBuffer[(String)]()
    tuple.productIterator.foreach{i=> line += i.toString}
    line.mkString(split)
  }

  def main(args: Array[String]): Unit = {
    println(isDecimalNum("22"))
  }
}

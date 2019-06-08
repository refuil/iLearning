package com.xiaoi.common

//用于正则匹配
object RegexUtil {
  //去除最前和最后的双引号
  def replaceQuotations(s:String):String = {
    val pattern = "\"(.*)\"".r
    s match {
      case pattern(a) => a
      case _ => s
    }
  }
  //判断是否是数值类型，eg(2.1  .1  2. )
  def isAmount(s:String):Boolean = {
    val pattern = "[0-9]+(.[0-9]*)?|.{1}[0-9]+".r
    s match {
      case pattern(_) => true
      case _ => false
    }
  }
}

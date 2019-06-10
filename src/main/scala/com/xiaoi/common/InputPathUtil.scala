package com.xiaoi.common

import org.joda.time.DateTime

/**
 * 工具类，获取输入路径
 */

object InputPathUtil {

  val dayRegex = "(\\d{1,2})(d)".r
  val monthRegex = "(\\d{1,2})(m)".r
  val dateFormat = "yyyy/MM/dd"
  val monthFormat = "yyyy/MM"


  def getTargetDate(d: String): DateTime = {
    if (d == "0") (new DateTime()).plusDays(-1)
    else {
      val target = d.split("/")
      new DateTime(target(0).toInt, target(1).toInt, target(2).toInt, 0, 0, 0, 0)
    }
  }

  def getInputPath(days: Int, fromD: DateTime, rootPath: String): String = {
    val darr =
      for (d <- 1 to days)
        yield rootPath + "/" + fromD.plusDays(0 - d).toString(dateFormat)
    val validInputs = for(f <- darr;if (HDFSUtil.exists(rootPath, f))) yield f
    println("input path:")
    println(validInputs.mkString("\n"))
    validInputs.mkString(",")
  }

  /**
    * 获取输入路径,根路径为path,
    * period可以按天指定,例如 10d, 30d
    * 也可以按月指定,例如 6m, 12m
    * 从fromD开始向前推peroid指定的时间
    * 如果period为其他值,则返回path值
    * @param period
    * @param fromD
    * @param path
    * @return
    */
  def getInputPath(period: String, fromD: DateTime, path: String): String = {
    val result = period match {
      case dayRegex(days, _) =>
        val paths = (1 to days.toInt).map(i => s"$path/" + fromD.plusDays(0 - i).toString(dateFormat))
        val validInputs = paths.filter(p => HDFSUtil.exists(path, p))
        println(validInputs.mkString("\n"))
        validInputs.mkString(",")
      case monthRegex(months, _) =>
        val paths = (0 to months.toInt).map(i => s"$path/" + fromD.minusMonths(i).toString(monthFormat))
        val validInputs = paths.filter(p => {
          println(p)
          HDFSUtil.exists(path, p)})
        println(validInputs.mkString("\n"))
        validInputs.map(x => x + "/*").mkString(",")
      case _ => path
    }
    result
  }

  /**
    * 根据天数获得数据源
    * @param days
    * @param targetDate
    * @param rootPath
    * @return
    */
  def getInputDataPathByDays(days : Int, targetDate : String, rootPath : String) : String = {
    val targetDateTime =
      if(targetDate == "0") new DateTime()
      else {
        val target = targetDate.split("/")
        (new DateTime(target(0).toInt, target(1).toInt, target(2).toInt, 0, 0, 0, 0)).plusDays(1)
      }
    val dateFormat = "yyyy/MM/dd"
    val darr =
      for (d <- 1 to days)
        yield rootPath + "/" + targetDateTime.plusDays(0 - d).toString(dateFormat)
    val validInputs = for(f <- darr;if (HDFSUtil.exists(rootPath, f))) yield f
    println("input path:")
    println(validInputs.mkString(","))
    validInputs.mkString(",")
  }
}

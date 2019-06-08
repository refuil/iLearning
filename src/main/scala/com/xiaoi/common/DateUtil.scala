package com.xiaoi.common

import java.text.SimpleDateFormat

import scala.collection.mutable._

object DateUtil {

  //获取日期,格式化时间字符串
  def getDate(date: String = "auto", datePattern: String = "yyyy-MM-dd HH:mm:ss"): String = {
    try {
      if (date == "auto") DateTime.now.toString("yyyy-MM-dd")
      else DateTimeFormat.forPattern(datePattern).parseDateTime(date).toString("yyyy-MM-dd")
    } catch {
      case _: Throwable => "1970-01-01"
    }
  }

  //获取准确时间
  def getTime(date: String = "auto", datePattern: String = "yyyy-MM-dd HH:mm:ss"): String = {
    try {
      if (date == "auto") DateTime.now.toString("yyyy-MM-dd HH:mm:ss")
      else DateTimeFormat.forPattern(datePattern).parseDateTime(date).toString("yyyy-MM-dd HH:mm:ss")
    } catch {
      case _: Throwable => "1970-01-01"
    }
  }

  //计算时间差 Millisecond
  def dateTimeDiffInMil(beginDateTime: String, endDateTime: String, datePattern: String = "yyyy-MM-dd HH:mm:ss"): Long = {
    try {
      DateTimeFormat.forPattern(datePattern).parseDateTime(endDateTime).getMillis -
        DateTimeFormat.forPattern(datePattern).parseDateTime(beginDateTime).getMillis
    } catch {
      case _: Throwable => 0
    }
  }

  //计算时间差 Days
  def dateTimeDiffInDay(beginDateTime: String, endDateTime: String, datePattern: String = "yyyy-MM-dd HH:mm:ss"): Long = {
    try {
      math.abs(DateTimeFormat.forPattern(datePattern).parseDateTime(endDateTime).getMillis -
        DateTimeFormat.forPattern(datePattern).parseDateTime(beginDateTime).getMillis)/ 86400000
    } catch {
      case _: Throwable => 0
    }
  }

  /**
    * 用出生日期计算年龄  返回string
    * @param birth
    * @return
    */
  def getAgeFromBirth(birth: String) :String= {
    if (birth == null || "".equals(birth) || birth.size < 6) ""
    else {
      //      val format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
      //      //时间解析
      //      val birthday = DateTime.parse(birth, format)
      //      val now = new LocalDate(DateTimeZone.forID("+08:00"))
      ////      val period = new Period(birthday, now, PeriodType.yearMonthDay())
      //      (now - birthday)
      //      period.getYears() + ""
      (DateTime.now.toString("yyyy-MM-dd HH:mm:ss").substring(0, 4).toInt - birth.substring(0, 4).toInt).toString
    }
  }

  /**
    * Time to timestamp
    * @param date
    * @return
    */
  def getTimestamp(date: String): Long = {
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date).getTime
  }

  /**
    * 获得当前日之前某一天的时间，如2017-08-02
    * @param auto
    * @param days
    * @return
    */
  def getDateBefore(auto: String, days: Int): String ={
    if (auto == "auto") {
      var date = DateTime.now
      (date - days.days).toString("yyyy-MM-dd")
    }else{
      "1970-01-01"
    }
  }

  /**
    * 根据给定的日期往前推若干天
    * @param time
    * @param days
    * @return
    */
  def getTimeBefore(time: String, days: Int): String ={
    var date = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").parseDateTime(time)
    val yesterday = (date - days.days).toString("yyyy-MM-dd HH:mm:ss")
    yesterday
  }

  /**
    * 获得当前日之前某一天的时间，如2017-08-02
    * @param days
    * @return
    */
  def getDateBefore(days: Int): String ={
    var date = DateTime.now
    val yesterday = (date - days.days).toString("yyyy-MM-dd")
    yesterday
  }

  /*
  *   参数是一个时间戳数组
  *   返回值是 这四个week,hour,month,season的Map元组
  */
  def time_count(arr:Array[Long]):(Map[Int,Int],Map[Int,Int],Map[Int,Int],Map[Int,Int]) = {
    //1-7分别代表星期一 ==>星期日
    val week_map = Map[Int,Int]()
    for (i <- 1 to 7) {
      week_map.put(i,0)
    }
    //时间的6-23
    val hour_map = Map[Int,Int]()
    for (i <- 6 until 24){
      hour_map.put(i,0)
    }
    //月份的1-12
    val month_map = Map[Int,Int]()
    for (i <- 1 to 12){
      month_map.put(i,0)
    }
    val season_map = Map[Int,Int]()
    //1，2，3，4分别代表春夏秋冬
    for (i <- 1 to 4){
      season_map.put(i,0)
    }
    arr.map(x=>{
      val hour = DateTimeFormat.forPattern("HH").print(x).toInt
      val month = DateTimeFormat.forPattern("MM").print(x).toInt
      val week = DateTimeFormat.forPattern("E").print(x)
      week match {
        case "星期一" => week_map.put(1,week_map(1)+1)
        case "星期二" => week_map.put(2,week_map(2)+1)
        case "星期三" => week_map.put(3,week_map(3)+1)
        case "星期四" => week_map.put(4,week_map(4)+1)
        case "星期五" => week_map.put(5,week_map(5)+1)
        case "星期六" => week_map.put(6,week_map(6)+1)
        case "星期日" => week_map.put(7,week_map(7)+1)
        case _ => 0
      }
      //小时
      if (hour < 24 && hour >= 6){
        hour_map.put(hour,hour_map(hour)+1)
      }
      //月份
      month_map.put(month,month_map(month)+1)
      //季节
      if (month <= 5 && month >= 3){
        season_map.put(1,season_map(1)+1)
      }else if (month <= 8 && month >= 6){
        season_map.put(2,season_map(1)+1)
      }else if (month <= 11 && month >= 9){
        season_map.put(3,season_map(1)+1)
      }else{
        season_map.put(4,season_map(1)+1)
      }
    })
    //星期，小时，月份，季节 都是Map
    (week_map,hour_map,month_map,season_map)
  }
  def week_count(arr:Array[Long]) = {
    //一个星期中每天的交易记录数量
    val week_map = Map[Int, Int]()
    //1-7分别代表星期一 ==>星期日
    for (i <- 1 to 7) {
      week_map.put(i, 0)
    }
    arr.map(x => {
      //星期几
      val week = DateTimeFormat.forPattern("E").print(x)
      week match {
        case "星期一" => week_map.put(1, week_map(1) + 1)
        case "星期二" => week_map.put(2, week_map(2) + 1)
        case "星期三" => week_map.put(3, week_map(3) + 1)
        case "星期四" => week_map.put(4, week_map(4) + 1)
        case "星期五" => week_map.put(5, week_map(5) + 1)
        case "星期六" => week_map.put(6, week_map(6) + 1)
        case "星期日" => week_map.put(7, week_map(7) + 1)
        case _ => 0
      }
    })
    week_map
  }

  def getWeekDayFromStr(timeStr: String)={
    val week = DateTimeFormat.forPattern("E")
      .print(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timeStr).getTime)
    week match {
      case "星期一" => 1
      case "星期二" => 2
      case "星期三" => 3
      case "星期四" => 4
      case "星期五" => 5
      case "星期六" => 6
      case "星期日" => 7
      case _ => 0
    }
  }

  def main(args: Array[String])={

    println(getWeekDayFromStr("2018-01-01 12:02:12"))
  }
}

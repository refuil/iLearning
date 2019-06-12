package com.xiaoi.common

import java.text.SimpleDateFormat
import com.github.nscala_time.time.Imports._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

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

}

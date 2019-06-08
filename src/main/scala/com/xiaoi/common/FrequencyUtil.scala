package com.xiaoi.common

import scala.collection.mutable.ListBuffer

/*
* Array数组都是距离观察点的天数的数组
* days是可变参，距离观察点的天数
* 返回值是，每个相应维度的次数
*/
object FrequencyUtil {
  //最近num天，每天的交易记录数
  def frequecy_count(days:Int,sldat_Array:List[Int]) ={
    val year_count = frequecy_year_count(sldat_Array)
    val month_count = frequecy_month_count(sldat_Array)
    val day_count = frequecy_day_count(days,sldat_Array)
    (year_count,month_count,day_count)
  }
  def num_days_total_money(days:Int,money_Array:List[Tuple2[Int,Double]]) ={
    val money_year = total_money_year(money_Array)
    val money_month = total_money_month(money_Array)
    val money_day = total_money_day(days,money_Array)
    (money_year,money_month,money_day)
  }
  //最近num天，前n天的交易记录和
  def num_days_total(days:Int,sldat_Array:List[Int]) ={
    var num_days_count = ListBuffer[Double]()
    for (i <- Array(1,3,7,15)){  //20181022 change: 0 until days
      var count = 0
      sldat_Array.map(x=>{
        if (x <= i) {
          count += 1
        }
        count
      })
      num_days_count.append(count)
    }
    num_days_count
  }
  //每#年的**次数
  def frequecy_year_count(sldat_Array:List[Int]) = {
    val num_days_count_year = ListBuffer[Int]()
    val year = Constants.YEAR_PART
    for (i <- 1 until year.length){
      var count_year = 0
      sldat_Array.map(x=>{
        if (x < year(i) && x >= year(i-1)) {
          count_year += 1
        }
        count_year
      })
      num_days_count_year.append(count_year)
    }
    num_days_count_year
  }
  //每月的**次数
  def frequecy_month_count(sldat_Array:List[Int]) = {
    val num_days_count_month = ListBuffer[Int]()
    val month = Constants.MONTH_PART
    for (i <- 1 until month.length){
      var count_month = 0
      sldat_Array.map(x=>{
        if (x < month(i) && x >= month(i-1)) {
          count_month += 1
        }
        count_month
      })
      num_days_count_month.append(count_month)
    }
    num_days_count_month
  }
  //每天的**次数
  def frequecy_day_count(days:Int,sldat_Array:List[Int]) = {
    val num_days_count_day = ListBuffer[Double]()
    for (i <- 0 until days){
      var count_day = 0
      sldat_Array.map(x=>{
        if (x == i) {
          count_day += 1
        }
        count_day
      })
      num_days_count_day.append(count_day)
    }
    num_days_count_day
  }
  //每#年的消费金额
  def total_money_year(money_Array:List[Tuple2[Int,Double]]) = {
    val num_days_total_money_year = ListBuffer[Double]()
    val year = Constants.YEAR_PART
    for (i <- 1 until year.length){
      var total_money_year = 0.0
      money_Array.map(x=>{
        if (x._1 < year(i) && x._1 >= year(i-1)){
          total_money_year += x._2
        }
        total_money_year
      })
      num_days_total_money_year.append(total_money_year)
    }
    num_days_total_money_year
  }
  //每月的消费金额
  def total_money_month(money_Array:List[Tuple2[Int,Double]]) = {
    val num_days_total_money_month = ListBuffer[Double]()
    val month = Constants.MONTH_PART
    for (i <- 1 until month.length){
      var total_money_month = 0.0
      money_Array.map(x=>{
        if (x._1 < month(i) && x._1 >= month(i-1)){
          total_money_month += x._2
        }
        total_money_month
      })
      num_days_total_money_month.append(total_money_month)
    }
    num_days_total_money_month
  }

  def total_money_day(days:Int,money_Array:List[Tuple2[Int,Double]]) = {
    var num_days_total_money_day = ListBuffer[Double]()
    for (i <- 0 until days){
      var total_money_day = 0.0
      money_Array.map(x=>{
        if (x._1 == i){
          total_money_day += x._2
        }
        total_money_day
      })
      num_days_total_money_day.append(total_money_day)
    }
    num_days_total_money_day
  }

}

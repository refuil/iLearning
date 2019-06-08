package com.xiaoi.common

object StatsUtil {
  /**
    * 平均值 中位数 众数 标准差
    * @param arr
    * @return
    */
  def mean(arr: Array[Double]): Double = {
    if(arr.isEmpty)0
    else arr.sum / arr.length.toDouble
  }//.formatted("%.2f").toDouble
  def media(arr: Array[Double]): Double = {
    val sorted = arr.sorted
    arr.length match{
      case len if len == 0   => 0
      case len if len % 2 == 0  => (sorted(len /2 -1) + sorted(len /2)) /2.0
      case len  => sorted(len /2)
    }
  }//.formatted("%.2f").toDouble
  def mode(arr:Array[Int]): Int = {
    val map = arr.foldLeft(Map.empty[Int, Int]) {(m, a) => m + (a -> (m.getOrElse(a,0) + 1))}
    map.maxBy(_._2)._1
  }
  def std(arr: Array[Double]): Double ={
    val avg = mean(arr)
    val variance = arr.map(x => math.pow(x - avg, 2)).sum / arr.size
    math.sqrt(variance)
  }

  /**
    * 任意两个数值字符取商得到Double字符
    * Long/Long = Double%.2F
    */
  def double_ratio(count:Any, total_sum:Any):Double = {
    count.toString.toDouble / total_sum.toString.toDouble//.formatted("%.4f").toDouble
  }

  //排序，求出最小值、最大值、中位数、平均值，标准差
  def sta_Count(arr:Array[Double]) = {
    val splitStr = "|"
    //平均值
    val avg = mean(arr)
    //标准差
    val std_num = arr.map(x => math.pow(x - avg, 2)).sum / arr.size
    //最大值
    val max_num = arr.max
    //最小值
    val min_num = arr.min
    //中位数
    var midleNum = media(arr)
    List(min_num,max_num,midleNum,avg,std_num).mkString(splitStr)
  }
}

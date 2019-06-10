package com.xiaoi.spark.util

/**
  * Function: 通过FPG算法获取数据
  * Author: shuangfu.zhang
  */

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD

object FPGAnalyse {

  def analyse(dataLines: RDD[String], splitChar: String, minSupport: Double,
              minConfidence: Double, minLift: Double, numPartition: Int): RDD[String] = {
    val transactions = dataLines
      .map(_.split(splitChar))
    val fpg = new FPGrowth()
      .setMinSupport(minSupport)
      .setNumPartitions(numPartition)

    val model = fpg.run(transactions)

    //添加信任度和提升度
    var result = model.freqItemsets

    //返回结果
    val ret = result
      .filter(itemset => itemset.items.length >= 2)
      .map(x => x.items(0) + "|" + x.items(1) + "|" + x.freq)
    ret
  }

}

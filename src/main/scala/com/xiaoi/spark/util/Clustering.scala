package com.xiaoi.spark.util

import com.xiaoi.common.{CalSimilar, FilterUtils, Segment}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.HashSet

/**
  * Created by josh on 8/13/17.
  * Params t1 > t2, where data source limited, lower t1 and t2
  */
object Clustering {
  def getQuesCluster(source: RDD[(String)], stopFilePath: String, t1: Double, t2: Double): RDD[(String, Long)] ={
    val data = source.map(x => (x, 1))
      .partitionBy(new HashPartitioner(8))
      .map(_._1)
      .distinct()
      .map(x => {
        Segment.init(true, stopFilePath, false, "")
        val seg = Segment.segment(FilterUtils.replaceSymbolToSpace(x), " ")
        (x, x, seg)
      })
      .zipWithIndex()
      .map(x => (x._2, x._1))
    val mapCenters = new mutable.HashSet[(Long, (String, String, String))]
    val rawCenter = data
      .filter(x => FilterUtils.chineseRatioCheck(x._2._1, 0.3))
      .partitionBy(new HashPartitioner(1)) //如需提高性能可以考虑针对问句长度做partition
      .map(v => (v._1, getCenters(v, mapCenters, t2)))
      .filter(a => a._2 != null)
      .collect()
      .toList
    val cluster = data
      .filter(x => FilterUtils.chineseRatioCheck(x._2._1, 0.3))
      .partitionBy(new HashPartitioner(32))
      .flatMap(x => {
        rawCenter.map(y => {
          val dis = getDistance(x._2, y._2._2)
          if(dis <= t1) (y._2._2._1, x._2._1, dis)
          else ("", "", 1.1)
        })
      })
      .filter(x => x._3 != 1.1)
      .map(x => (x._1, x._2))
      .groupByKey()
      .map(x => (List(x._1, x._2.mkString("|")).mkString("|"), x._2.toList.length,
        x._2.mkString("").length.toDouble / x._2.toList.length))
      .sortBy(_._3, false)
      .zipWithIndex()
      .flatMap(x => {
        x._1._1.split("\\|").map(y => (y, x._2)).toSeq
      })
      .groupByKey()
      .map(x => (x._1, x._2.toList.sortWith((a, b) => a <= b)(0)))
      .cache()

    val lastClusterId =
      if(cluster.count() > 0) cluster.sortBy(_._2, false).take(1)(0)._2
      else -1

    val noCluster = data.map(x => (x._2._1, lastClusterId.toLong + 1))
      .leftOuterJoin(cluster)
      .filter(x => x._2._2 == None)
      .map(x => (x._1, x._2._1))

    cluster
      .union(noCluster)
      .sortBy(x => x._2)
      .zipWithIndex()
      .filter(x => x._2 <= 1000)
      .map(_._1)
  }

  /**
    * 计算距离，相似度取编辑距离和jaccard的最大值
    * （初始问句，经过处理的问句，问句分词）
    * @param q1
    * @param q2
    * @return
    */
  def getDistance(q1 : (String, String, String), q2 : (String, String, String)): Double = {
    val jac = CalSimilar.calJaccardSimilar(q1._3, q2._3, " ")
    val edit = CalSimilar.calEditSimilar(q1._2.replace(" ",""), q2._2.replace(" ",""))
    1.0 - scala.math.max(edit, jac)
  }

  /**
    * 粗略得到聚类中心
    * 如问句不属于任何已有到聚类（与中心的距离小于t2），该问句就做为新的聚类中心
    * @param question
    * @param mapCenter
    * @param t2
    * @return
    */
  def getCenters(question : (Long, (String, String, String)), mapCenter : HashSet[(Long, (String, String, String))],
                 t2 : Double) : (Long, (String, String, String)) = {
    if (!mapCenter.exists(q => getDistance(q._2, question._2) < t2)) {
      mapCenter += question
      question
    } else null
  }
  
}


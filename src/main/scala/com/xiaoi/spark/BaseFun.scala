package com.xiaoi.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.collection.Map

trait BaseFun {
  val logger = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  val mongoDBName = "recommender"
  val mongoCollectName = "rec_result"
  val mongoUser = "admin"
  val mongoPwd = "dis2019"
  val mongoUriStr = s"mongodb://${mongoUser}:${mongoPwd}@122.226.240.140:27018/${mongoDBName}"

  /**
    * Method logging
    * @param str
    */
  def logging(str: String)={
    logger.info(str)
  }

  /**
    * Get map data type
    */
  def getMapData(dataAlias: RDD[String]): Map[String, String] =
    dataAlias.flatMap { line =>
      val tokens = line.split("\\|")
      if (tokens(0).isEmpty) {
        None
      } else {
        Some((tokens(0), tokens(1)))
      }
    }.collectAsMap()

  /**
    * idAlias to id
    * @param resultData
    * @param bUserAlias
    * @param bItemAlias
    * @return
    */
  def transUIAlias(resultData: RDD[String],
                 bUserAlias:Broadcast[Map[String,String]],
                 bItemAlias: Broadcast[Map[String,String]]) =
    resultData.map { line =>
      val Array(userID, itemID, count) = line.split("\\|",-1)
      val finalUserID = bUserAlias.value.getOrElse(userID,"")
      val finalItemID = bItemAlias.value.getOrElse(itemID, "")
      (finalUserID,finalItemID, count)
    }

  /**
    * id transform
    * @param itemID
    * @param bItemAlias
    * @return
    */
  def transAliasFun(itemID: String,
                 bItemAlias: Broadcast[Map[String,String]]) =
      bItemAlias.value.getOrElse(itemID, "")

}

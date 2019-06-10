package com.xiaoi.spark

import java.io.File

import com.xiaoi.spark.question.MainBatch.Params
import org.apache.spark.sql.{Dataset, SparkSession}
import org.ini4j.Ini

import scala.util.Random

/**
  * The parent of all offline
  * created by josh on 201903
  */
abstract class BaseOffline extends BaseFun with Serializable {

  /**
    * 实际处理逻辑，子类需要覆盖此方法
    * @param lines
    */
  def process(lines: Dataset[String], params: Params): Unit

  /**
    * 加载Ini配置文件
    * @param params
    * @return
    */
  def loadIni(params: Params): Ini = {
    val ini = new Ini()
    ini.load(new File((params.configIni)))
    ini
  }

  /**
    * 数据放大指定倍数,并加上随机数
    * @param oriValue
    * @param factor
    * @return
    */
  def valueMock(oriValue: Long, factor: Int): Long = {
    val random = new Random(System.currentTimeMillis())
    val rand = random.nextInt(factor)
    oriValue * factor + rand
  }

  /**
    * read userAction data from csv file
    * @param spark
    * @param inputPath
    * @return
    */
  def readActionCsv(spark: SparkSession,
                    inputPath: String) ={
    spark.read.
      option("header", "true").
      option("sep", ",").
      format("csv").
      load(inputPath)
  }

}

package com.xiaoi.spark.example

import java.io.PrintWriter

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

import scala.io.Source

/**
  * CorpusAndIDF
  * 从语料库中获取词汇表和IDF特征以及TF-IDF特征
  *
  * Created by ligz on 16/3/21.
  */
class CorpusAndIDF(corpusPath: String, objOutput: Boolean = true, toLocal: Boolean = false) extends Serializable {

  val VOCAB = "vocab"
  val IDF = "idf"

  var vocabulary: Map[String, Long] = null

  var idf: Map[Long, Double] = null

  var numFeatures: Int = 100000

  def save(sc: SparkContext): Unit = {

    assert(!vocabulary.isEmpty, "Vocabulary is empty")
    assert(!idf.isEmpty, "IDF is empty")

    if (!toLocal) { // 保存到hdfs上
      if (objOutput) {  // 保存为object格式
        sc.parallelize(vocabulary.toSeq).saveAsObjectFile(s"$corpusPath/$VOCAB")
        sc.parallelize(idf.toSeq).saveAsObjectFile(s"$corpusPath/$IDF")
      } else {   // 保存为文本格式
        sc.parallelize(vocabulary.toSeq).saveAsTextFile(s"$corpusPath/$VOCAB")
        sc.parallelize(idf.toSeq).saveAsTextFile(s"$corpusPath/$IDF")
      }
    } else {  // 保存到本地
      val vocabWriter = new PrintWriter(s"$corpusPath/$VOCAB")
      val idfWriter = new PrintWriter(s"$corpusPath/$IDF")
      vocabulary.foreach(x => vocabWriter.println(x._1 + "|" + x._2.toString))
      vocabWriter.close()
      idf.foreach(x => idfWriter.println(x._1 + "|" + x._2))
      idfWriter.close()
    }

  }

  def load(sc: SparkContext): Unit = {
    if (!toLocal) {
      if (objOutput) {
        vocabulary = sc.objectFile[(String, Long)](s"$corpusPath/$VOCAB").collect.toMap
        idf = sc.objectFile[(Long, Double)](s"$corpusPath/$IDF").collect.toMap
      } else {
        val wordIdReg = "^\\((.*),(\\d+)\\)$".r
        vocabulary = sc.textFile(s"$corpusPath/$VOCAB").map(line => {
          val wordIdReg(word, wid) = line
          (word, wid.toLong)
        }).collect.toMap
        val wordIdIDFReg = "^\\((\\d+),(.*)\\)$".r
        idf = sc.textFile(s"$corpusPath/$IDF").map(line => {
          val wordIdIDFReg(wid, idfV) = line
          (wid.toLong, idfV.toDouble)
        }).collect().toMap
      }
    } else {
      vocabulary = Source.fromFile(s"$corpusPath/$VOCAB").getLines().
        map(_.split("\\|")).filter(_.length == 2)
        .map(item => item(0) -> item(1).toLong).toMap
      idf = Source.fromFile(s"$corpusPath/$IDF").getLines().
        map(_.split("\\|")).filter(_.length == 2)
        .map(item => item(0).toLong -> item(1).toDouble).toMap
    }
    numFeatures = vocabulary.size
  }


  def getVocabAndIDF(docs: RDD[List[String]], maxNumFeat: Int = 100000): Unit = {
    docs.cache()
    vocabulary = docs.flatMap(x => x).map(_ -> 1).reduceByKey(_ + _)
      .sortBy(-_._2).zipWithIndex().take(maxNumFeat)
      .map(_.swap).map(x => (x._2._1, x._1))
      .toMap

    numFeatures = vocabulary.size

    val docCnt = docs.count()

    idf = docs.map(_.distinct)
      .flatMap(x => x.map(word => vocabulary.getOrElse[Long](word, -1)).filter(_ >= 0))
      .groupBy(x => x)
      .map(item => item._1 -> Math.log(docCnt * 1.0 / (item._2.size + 1))).collect().toMap
  }

  def calcTFIDF(doc: List[String]): Vector = {
    val wordCnt = doc.length
    val tf = doc.map(word => vocabulary.getOrElse[Long](word, -1)).filter(_ >= 0)
      .groupBy(x => x).map(item => item._1 -> item._2.length * 1.0 / wordCnt)
    val tfidf = tf.map(item => item._1.toInt -> item._2 * idf.getOrElse(item._1, 3.76)).toSeq

    Vectors.sparse(numFeatures, tfidf).toDense
  }

}


object CorpusAndIDF {

  def apply(corpusPath: String) = new CorpusAndIDF(corpusPath)

}

package com.xiaoi.spark.artificial_v1

import com.xiaoi.common.{DBUtil, HDFSUtil, StrUtil}
import com.xiaoi.conf.ConfManager
import com.xiaoi.constant.Constants
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import scopt.OptionParser

import scala.collection.mutable.ListBuffer

/**
 * 合并fastText的推荐结果
 * Created by ligz on 16/08/22.
 */
object UniteFastTextResult {

  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName("UniteFastTextResult")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val inPath = params.inPath
    val nClasses = params.nClasses
    val batchIdFile = params.batchIdFile
    val url = params.dburl
    val username = params.username
    val pwd = params.pwd
    val table = params.table
    val withProb = params.withProb
    val numPartitions = params.numPartitions
    val faqIdPath = params.faqIdPath

    val batchId = scala.io.Source.fromFile(batchIdFile).getLines().toList(0)
    val targetDate = DateTime.parse(batchId, DateTimeFormat.forPattern("yyyyMMdd_HHmmss")).toString("yyyy-MM-dd")

    //读取标准问和ID映射
    val faqMap = if(HDFSUtil.exists(faqIdPath, faqIdPath)){
      sc.textFile(faqIdPath).map(x=>{
        val splits = x.split("\\|")
        (splits(0), splits(1))
      })
    }else{
      sc.parallelize(List(("","")))
    }

    /**
      * 解析fastText返回结果的每一行,得到聚类id,推荐标准问,置信度三元组
      *
      * @param line
      * @return
      */
    def parseLine(line: String): List[(String, String, Double)] = {
      val unans_result = line.split("\\t")
      val cid = unans_result(0)
      unans_result.takeRight(1)(0).split("__label__").drop(1).map(one_result => {
        val fields = one_result.split(" ")
        val prob = if (withProb) fields(1).toDouble else 1.0
        val faq = fields(0).replace("_sp_", " ")
        (cid, faq, prob)
      }).toList
    }

    val lr_1 = sc.textFile(inPath)
      .flatMap(parseLine)

    //（CLUSTER_ID, FAQ_IDX, RECOM_FAQ）
    val recomm = lr_1.map(x => ((x._1, x._2), x._3))
      .reduceByKey(_ + _)
      .map(x => (x._1._1, List[(String, Double)]((x._1._2, x._2))))
      .reduceByKey(_ ++ _, numPartitions)
      .mapValues(iter => {
        val n_recomm = iter.sortBy(-_._2).take(nClasses)
        val len = n_recomm.length
        val recomm = new ListBuffer[(String, Double, Int)]
        for (i <- 0 to (len - 1)) {
          val (faq, confidence) = n_recomm(i)
          recomm += Tuple3(faq, confidence, i + 1)
        }
        recomm.toList
      })
      .flatMapValues(x => x)
      .map(x=>(x._1, x._2._3, x._2._1))
//      .map(x => (uuid, targetDate, x._1, x._2._3, x._2._1, "", "", batchId))

    //标准问关联得到标准问ID （CLUSTER_ID, FAQ_IDX, RECOM_FAQ， FAQ_ID）
    val faqRecomm = recomm.map(x=> (x._3, (x._1, x._2))).leftOuterJoin(faqMap).map(x=>(x._2._1._1, x._2._1._2, x._1, x._2._2.getOrElse("")))

    val recommList= faqRecomm.collect().toList
    recommList.take(20).foreach(println)
//    import sqlContext.implicits._
//    val faqDF = recomm.toDF("ID", "CREATE_DATE", "CLUSTER_ID", "FAQ_IDX",
//      "RECOM_FAQ", "FAQ_ID", "FAQ_ANS", "BATCHID")
    val deleteSql = s"delete from $table where BATCHID = ?"
    val insertSql = s"insert into $table (ID, CREATE_DATE, CLUSTER_ID, FAQ_IDX, RECOM_FAQ, FAQ_ID, FAQ_ANS, BATCHID) values (?,?,?,?,?,?,?,?)"
    val conn = DBUtil.getConnection(url, username, pwd, ConfManager.getString(Constants.JDBC_DRIVER))
    try {
      conn.setAutoCommit(false)
      //删除当前批次数据，防止重复写入
      val delPst = conn.prepareStatement(deleteSql)
      delPst.setString(1,batchId)
      delPst.execute()
      val insertPst = conn.prepareStatement(insertSql)
      recommList.foreach(x=>{
        insertPst.setString(1, StrUtil.uuid())
        insertPst.setString(2, targetDate)
        insertPst.setString(3, x._1)
        insertPst.setInt(4, x._2)
        insertPst.setString(5, x._3)
        insertPst.setString(6, x._4)
        insertPst.setString(7, "")
        insertPst.setString(8, batchId)
        insertPst.execute()
      })
    }catch{
      case e:Exception =>  {
        conn.rollback()
        //        e.printStackTrace()
        throw new Exception(e)
      }
    }finally{
      if(conn != null){
        conn.commit()
        conn.setAutoCommit(true)
        conn.close()
      }
    }

//    faqDF.show(20, false)
//    JdbcHelp.update(url, username, pwd, faqDF, table)
    sc.stop()
  }


  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("Generate unanswered-questions recommend-faq") {
      head("Generate unanswered-questions recommend-faq")
      opt[String]('l', "in_path")
        .text("fastText recommend path")
        .action((x, c) => c.copy(inPath = x))
      opt[String]("batchIdFile")
        .text("批次ID文件")
        .action((x, c) => c.copy(batchIdFile = x))
      opt[Boolean]("delPrev")
        .text("是否删除已经插入数据库的当天执行结果")
        .action((x, c) => c.copy(delPrev = x))
      opt[Boolean]("withProb")
        .text("分类结果是否包含每个分类的概率值")
        .action((x, c) => c.copy(withProb = x))
      opt[Int]("numPartitions")
        .text("分区个数,建议等于集群中Worker个数")
        .action((x, c) => c.copy(numPartitions = x))
      opt[Int]('n', "nClasses")
        .text("recommend n classes")
        .action((x, c) => c.copy(nClasses = x))
      opt[String]("faqIdPath")
        .text("faqIdPath")
        .action((x, c) => c.copy(faqIdPath = x))
      opt[String]("dburl")
        .text("db connection url")
        .action((x, c) => c.copy(dburl = x))
      opt[String]("username")
        .text("db username")
        .action((x, c) => c.copy(username = x))
      opt[String]("pwd")
        .text("db password")
        .action((x, c) => c.copy(pwd = x))
      opt[String]("table")
        .text("db table name")
        .action((x, c) => c.copy(table = x))
      checkConfig { params => success }

    }

    parser.parse(args, defaultParams).map {
      params => run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  case class Params(
                     inPath: String = "",
                     batchIdFile: String = "",
                     delPrev: Boolean = false,
                     withProb: Boolean = false,
                     nClasses: Int = 5,
                     numPartitions: Int = 4,
                     faqIdPath: String = "",
                     dburl: String = "jdbc:mysql://172.16.0.34:3306/lenovo_analysis",
                     username: String = "lenovo",
                     pwd: String = "lenovo_Analysis1102",
                     table: String = "QA_FAQ")

}

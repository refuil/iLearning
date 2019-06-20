package com.xiaoi.spark.artificial_v1

import java.util.Properties

import com.xiaoi.common.{DBUtil, StrUtil}
import com.xiaoi.conf.ConfManager
import com.xiaoi.constant.Constants
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import scopt.OptionParser

/**
  * 解析问题聚类结果,并插入到问题表中
  * Created by ligz on 16/08/22.
  */
object QuestionsInsert {

  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName("Insert_QA_Questions")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val inPath = params.inPath
    val batchIdFile = params.batchIdFile
    val url = params.dburl
    val username = params.username
    val pwd = params.pwd
    val quesTable = params.quesTable
    val clusterTable = params.clusterTable

    val batchId = scala.io.Source.fromFile(batchIdFile).getLines().toList(0)

    val targetDate = DateTime.parse(batchId, DateTimeFormat.forPattern("yyyyMMdd_HHmmss")).toString("yyyy-MM-dd")

//    if (params.delPrev) {
//      val deleteQues = s"delete from $quesTable where create_date = '$targetDate';"
//      val deleteFaq = s"delete from $clusterTable where create_date = '$targetDate';"
//      println(deleteQues)
//      println(deleteFaq)
//      val props = new Properties()
//      props.setProperty("driver", "com.mysql.jdbc.Driver")
//      props.setProperty("url", url)
//      props.setProperty("username", username)
//      props.setProperty("password", pwd)
//      println(SimpleJdbcOps.deleteBy(deleteQues, props))
//      println(SimpleJdbcOps.deleteBy(deleteFaq, props))
//    }

//    def uuid() = java.util.UUID.randomUUID().toString.replace("-", "")

//    import sqlContext.implicits._

    val input = sc.textFile(inPath).map {
      case (line) =>
        val fields = line.split("\\|")  // cluster_id, flag, weight, question_cnt, question
        (fields(0), fields(1), fields(2), fields(3), fields(4))
    }.cache()

    val questions = input.map {
      case (line) =>  // cluster_id, flag, weight, question_cnt, question
//        (uuid, targetDate, line._5, line._1, line._4.toInt, 0.0, 0.0, "unhandled", batchId)
        (line._5, line._1, line._4.toInt)
    }

    val clusters = input.map(line => (line._1, line._3.toDouble))
      .distinct()
      .map {
      case (cluster) => // cluster_id, weight
//        (uuid, targetDate, cluster._1, cluster._2.toDouble, "unhandled", batchId)
        (cluster._1, cluster._2.toDouble)
    }

    val quesList = questions.collect().toList
    val clusterList = clusters.collect().toList
    quesList.take(20).foreach(println)
    clusterList.take(20).foreach(println)
//    val quesDF = questions.toDF("ID", "CREATE_DATE", "UNANS_QUES", "CLUSTER_ID",
//      "UNANS_CNT", "OTHER_SCORE", "WEIGHT", "STATUS", "BATCHID")

    val deleteQuesSql = s"delete from $quesTable where BATCHID = ?"
    val deleteClusterSql = s"delete from $clusterTable where BATCHID = ?"
    val insertQuesSql = s"insert into $quesTable (ID, CREATE_DATE, UNANS_QUES, CLUSTER_ID, UNANS_CNT, OTHER_SCORE, WEIGHT, STATUS, BATCHID) values (?,?,?,?,?,?,?,?,?)"
    val insertClusterSql = s"insert into $clusterTable (ID, CREATE_DATE, CLUSTER_ID, CLUSTER_WEIGHT, STATUS, BATCHID) values (?,?,?,?,?,?)"
    val conn = DBUtil.getConnection(url, username, pwd, ConfManager.getString(Constants.JDBC_DRIVER))
    try {
      conn.setAutoCommit(false)
      //删除当前批次数据，防止重复写入
      val delQuesPst = conn.prepareStatement(deleteQuesSql)
      delQuesPst.setString(1,batchId)
      delQuesPst.execute()
      val delClusterPst = conn.prepareStatement(deleteClusterSql)
      delClusterPst.setString(1,batchId)
      delClusterPst.execute()
      //插入qa_question新数据
      val insertQuesPst = conn.prepareStatement(insertQuesSql)
      quesList.foreach(x=>{
        insertQuesPst.setString(1, StrUtil.uuid())
        insertQuesPst.setString(2, targetDate)
        insertQuesPst.setString(3, x._1)
        insertQuesPst.setString(4, x._2)
        insertQuesPst.setInt(5, x._3)
        insertQuesPst.setDouble(6, 0.0)
        insertQuesPst.setDouble(7, 0.0)
        insertQuesPst.setString(8, "unhandled")
        insertQuesPst.setString(9, batchId)
        insertQuesPst.execute()
      })
      //插入qa_cluster新数据
      val insertClusterPst = conn.prepareStatement(insertClusterSql)
      clusterList.foreach(x=>{
        insertClusterPst.setString(1, StrUtil.uuid())
        insertClusterPst.setString(2, targetDate)
        insertClusterPst.setString(3, x._1)
        insertClusterPst.setDouble(4, x._2)
        insertClusterPst.setString(5, "unhandled")
        insertClusterPst.setString(6, batchId)
        insertClusterPst.execute()
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

//    JdbcHelp.update(url, username, pwd, quesDF, quesTable)
//
//    val clusterDF = clusters.toDF("ID", "CREATE_DATE", "CLUSTER_ID", "CLUSTER_WEIGHT", "STATUS", "BATCHID")
////    clusterDF.show(200)
//    JdbcHelp.update(url, username, pwd, clusterDF, clusterTable)
    input.unpersist()
    sc.stop()
  }


  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("Generate unanswered-questions recommend-faq") {
      head("Generate unanswered-questions recommend-faq")
      opt[String]("inPath")
        .text("问题聚类数据HDFS路径")
        .action((x, c) => c.copy(inPath = x))
      opt[String]("batchIdFile")
        .text("批次ID文件")
        .action((x, c) => c.copy(batchIdFile = x))
      opt[Boolean]("delPrev")
        .text("是否删除已经插入数据库的当天执行结果")
        .action((x, c) => c.copy(delPrev = x))
      opt[String]("dburl")
        .text("db connection url")
        .action((x, c) => c.copy(dburl = x))
      opt[String]("username")
        .text("db username")
        .action((x, c) => c.copy(username = x))
      opt[String]("pwd")
        .text("db password")
        .action((x, c) => c.copy(pwd = x))
      opt[String]("quesTable")
        .text("db question table name")
        .action((x, c) => c.copy(quesTable = x))
      opt[String]("clusterTable")
        .text("db cluster table name")
        .action((x, c) => c.copy(clusterTable = x))
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
                     dburl: String = "",
                     username: String = "",
                     pwd: String = "",
                     quesTable: String = "",
                     clusterTable: String = "")

}

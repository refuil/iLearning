package com.xiaoi.spark.artificial_v1

import com.xiaoi.common.{DBUtil, StrUtil}
import com.xiaoi.conf.ConfManager
import com.xiaoi.constant.Constants
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import scopt.OptionParser

/**
  * 解析答案,并插入到答案表中
  * Created by ligz on 16/08/22.
  */
object AnswerInsert {

  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName("Insert_QA_Answers")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val inPath = params.inPath
    val batchIdFile = params.batchIdFile
    val url = params.dburl
    val username = params.username
    val pwd = params.pwd
    val table = params.table

    val batchId = scala.io.Source.fromFile(batchIdFile).getLines().toList(0)
    val targetDate = DateTime.parse(batchId, DateTimeFormat.forPattern("yyyyMMdd_HHmmss")).toString("yyyy-MM-dd")

//    if (params.delPrev) {
//      val sql = s"delete from $table where create_date = '$targetDate';"
//      val props = new Properties()
//      props.setProperty("driver", "com.mysql.jdbc.Driver")
//      props.setProperty("url", url)
//      props.setProperty("username", username)
//      props.setProperty("password", pwd)
//      println(sql)
//      println(SimpleJdbcOps.deleteBy(sql, props))
//    }

//    def uuid() = java.util.UUID.randomUUID().toString.replace("-", "")

//    import sqlContext.implicits._

    //（cluster_id, weight, answer）
    val questions = sc.textFile(inPath).map {
      case (line) =>
        val fields = line.split("\\|")  // cluster_id, weight, answer
//        (uuid, targetDate, fields(0), fields(2), fields(1), "unhandled", batchId)
        (fields(0), fields(1), fields(2))
    }

    val quesList = questions.collect().toList
    quesList.take(20).foreach(println)

    val deleteSql = s"delete from $table where BATCHID = ? "
    val answerSql = s"insert into $table (ID, CREATE_DATE, CLUSTER_ID, ANSWER, WEIGHT, STATUS, BATCHID) values (?,?,?,?,?,?,?)"
    val conn = DBUtil.getConnection(url, username, pwd, ConfManager.getString(Constants.JDBC_DRIVER))
    try {
      conn.setAutoCommit(false)
      //删除当前批次数据，防止重复写入
      val delPst = conn.prepareStatement(deleteSql)
      delPst.setString(1,batchId)
      delPst.execute()
      val insertPst = conn.prepareStatement(answerSql)
      quesList.foreach(x=>{
        insertPst.setString(1, StrUtil.uuid())
        insertPst.setString(2, targetDate)
        insertPst.setString(3, x._1)
        insertPst.setString(4, x._3)
        insertPst.setString(5, x._2)
        insertPst.setString(6, params.status)
        insertPst.setString(7, batchId)
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
//    val qDF = questions.toDF("ID", "CREATE_DATE", "CLUSTER_ID",
//      "ANSWER", "WEIGHT", "STATUS", "BATCHID")
//    JdbcHelp.update(url, username, pwd, qDF, table)
    sc.stop()
  }


  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("Generate unanswered-questions recommend-faq") {
      head("Generate unanswered-questions recommend-faq")
      opt[String]("inPath")
        .text("答案数据HDFS路径")
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
                     dburl: String = "",
                     username: String = "",
                     pwd: String = "",
                     table: String = "",
                     status: String = "unhandled"
                   )

}

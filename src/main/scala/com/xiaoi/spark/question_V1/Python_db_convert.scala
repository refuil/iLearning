package com.xiaoi.spark.question

import com.xiaoi.common.{DBUtil, DateUtil}
import com.xiaoi.conf.ConfigurationManager
import com.xiaoi.constant.Constants
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

/**
  * All Result Insert to database
  * Created by Josh on 7/18/17.
  */
object Python_db_convert {

  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName("Write into database")
    val sc = new SparkContext(conf)

    //存储未回答概要
    println(">>>>>Summery insert>>>>>>")
    unanswerRecommInsert(sc, params)
  }

  /**
    * 存储未回答概要
    * @param sc
    * @param params
    */
  def unanswerRecommInsert(sc: SparkContext, params: Params): Unit ={
    val data = sc.textFile(params.input_path + "/analysis_result.txt").map{x =>
      val list = x.split("\\|")
      //(row_id, ymd, unans_ques, weight, faq_id, faq, faq_path,
      //                  recomm_cnt, related_cnt, "unhandled", ori_ques,
      //                  center_id, center_weight, json.dumps(cnt_info),
      //                  month_info, is_center, summary, star)
      (list(0), list(1), list(2), list(3), list(4), list(5), list(6),
        list(7), list(8), list(9), list(10), list(11), list(12), list(13),
        list(14), list(15), list(16), list(17))
    }.filter(x => x._17.length < 100)
    val analysis_result = data
    //write to mysql
    val conn = DBUtil.getConnection(params.DBUrl, params.DBUser,
      params.DBPassword, ConfigurationManager.getString(Constants.JDBC_DRIVER))
    val deleteSQL = "delete from unans_semantic where CREATE_DATE = ?"
    val sql = "insert into unans_semantic (ID, CREATE_DATE, UNANS_QUES, WEIGHT, " +
      "RECOM_FAQ_ID, RECOM_FAQ, FAQ_PATH, RECOMM_FAQ_CNT, RELATED_CNT, STATUS, " +
      "ORI_QUES, CENTER_ID, CENTER_WEIGHT, CNT_INFO, MONTH_INFO, IS_CENTER, " +
      "SUMMARY, STAR) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    try {
      conn.setAutoCommit(false)
      //Delete data in this process
      val deletePrst = conn.prepareStatement(deleteSQL)
      deletePrst.setString(1, DateUtil.getDateBefore("auto", 1))
      deletePrst.execute()

      val wordPrst = conn.prepareStatement(sql)
      analysis_result.collect().toList.map(x => {
        wordPrst.setString(1, x._1)
        wordPrst.setString(2, x._2)
        wordPrst.setString(3, x._3)
        wordPrst.setString(4, x._4)
        wordPrst.setString(5, x._5)
        wordPrst.setString(6, x._6)
        wordPrst.setString(7, x._7)
        wordPrst.setString(8, x._8)
        wordPrst.setString(9, x._9)
        wordPrst.setString(10, x._10)
        wordPrst.setString(11, x._11)
        wordPrst.setString(12, x._12)
        wordPrst.setString(13, x._13)
        wordPrst.setString(14, x._14)
        wordPrst.setString(15, x._15)
        wordPrst.setString(16, x._16)
        wordPrst.setString(17, x._17)
        wordPrst.setString(18, x._18)
        wordPrst.addBatch()
      })
      wordPrst.executeBatch()
    } catch {
      case e: Exception => {
        conn.rollback()
        e.printStackTrace()
      }
    } finally {
      conn.commit()
      conn.setAutoCommit(true)
      if (conn != null) conn.close()
    }
  }

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("semantic write into db") {
      head("Write into DB")
      opt[String]('d', "input_path")
        .text("Input path for question data")
        .action((x, c) => c.copy(input_path = x))
      opt[String]('o', "output_path")
        .text("output to hdfs")
        .action((x, c) => c.copy(output_path = x))
      opt[String]('a' ,"DBHost")
        .text("dbhost")
        .action((x, c) => c.copy(DBHost = x))
      opt[String]('y', "DBUser")
        .text("DBUser")
        .action((x, c) => c.copy(DBUser = x))
      opt[String]('m', "DBPassword")
        .text("db password")
        .action((x, c) => c.copy(DBPassword = x))
      opt[String]('s', "DBName")
        .text("DBName")
        .action((x, c) => c.copy(DBName = x))
      opt[String]('p', "DBUrl")
        .text("db port")
        .action((x, c) => c.copy(DBUrl = x))
      checkConfig { params => success }
    }
    parser.parse(args, defaultParams).map {params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  case class Params(
                     input_path: String = "",
                     output_path: String = "",  //Local Output
                     DBHost: String = "",
                     DBUser: String = "",
                     DBPassword: String = "",
                     DBName: String = "",
                     DBUrl: String = "jdbc:MySQL://master:3306/robot"
                   )
}

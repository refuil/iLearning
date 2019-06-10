package com.xiaoi.spark.etl_v1

import java.io.File

import com.xiaoi.common.HadoopOpsUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.ini4j.Ini
import org.joda.time.DateTime
import scopt.OptionParser

/**
  * 从数据库中获取机器人日志
  * create by jxh 2018-04-12
  */
object GetLogsFromDatabase {

  case class Params(
                     configIni: String = "conf.ini",
                     ask_section: String = "ask_initial"
                   )

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("从数据库中获取机器人日志") {
      opt[String]("configIni")
        .text("配置文件路径")
        .action((x, c) => c.copy(configIni = x))
      opt[String]("ask_section")
        .text("数据导入方式,自动or指定时间段")
        .action((x, c) => c.copy(ask_section = x))
    }

    parser.parse(args, defaultParams).map {
      params => run(params)
    }.getOrElse(
      sys.exit(1)
    )
  }

  def run(params: Params): Unit = {
    //读取配置文件
    val ini = loadIni(params)
    val ask_section = params.ask_section

    println("配置文件内容查看-----" + ini)

    val host = ini.get("common", "host")
    val user = ini.get("common", "user")
    val password = ini.get("common", "password")
    val database = ini.get("common", "database")
    val port = ini.get("common", "port")
    val database_type = ini.get("common", "database_type")
    val table_prefix = ini.get(ask_section, "table_prefix")
    val target_dir = ini.get(ask_section, "target_dir")

    HadoopOpsUtil.removeOrBackup(target_dir, target_dir)
    val tempdir = "/tmp/xiaoi"
    HadoopOpsUtil.removeOrBackup(tempdir, tempdir)

    //根据数据库类型,选择driver和生成url
    val driver = if (database_type == "mysql") "com.mysql.jdbc.Driver" else "oracle.jdbc.OracleDriver"
    val url = if (database_type == "mysql") "jdbc:mysql://%s:%s/%s".format(host, port, database) else "jdbc:oracle:thin:@%s:%s:%s".format(host, port, database)

    println("database_type : " + database_type)
    println("host : " + host)
    println("user : " + user)
    println("database : " + database)
    println("port : " + port)
    println("DRIVER : " + driver)
    println("URL : " + url)

    //SQL模板,需要替换表名,开始时间,结束时间
    val sql = "select cast(trim(a.VISIT_TIME) as char(19)), a.SESSION_ID, a.USER_ID, a.QUESTION, a.QUESTION_TYPE, a.ANSWER, a.ANSWER_TYPE, " +
      "a.FAQ_ID, a.FAQ_NAME, a.KEYWORD, a.CITY, a.BRAND, a.SIMILARITY, a.MODULE_ID, a.PLATFORM ,a.EX , c.NAME as CATEGORY " +
      "from %s a left join kb_category c on a.CATEGORY_ID = c.ID  where (1 = 1) and a.VISIT_TIME between '%s' and '%s'"

    val conf = new SparkConf().setAppName("Get Logs From Database")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //获取kb_category表
    val kb_category = sqlContext.read.format("jdbc")
      .options(Map("driver" -> driver, "url" -> url, "user" -> user, "password" -> password, "dbtable" -> "kb_category")).load()
    kb_category.registerTempTable("kb_category")

    //sqlContext.sql("select * from kb_category").show()

    val pattern = "yyyy-MM-dd HH:mm:ss"

    //判断数据导入方式,自动or指定时间段
    //ask为自动导入昨天的数据,ask_initial自定义时间段
    if (ask_section == "ask") {
      //获取表名,开始时间,结束时间
      val yesterday = DateTime.now().plusDays(-1)
      val starttime = yesterday.secondOfDay().withMinimumValue().toString(pattern)
      val endtime = yesterday.secondOfDay().withMaximumValue().toString(pattern)
      val table = table_prefix + yesterday.toString("yyyyMM")

      val exec_sql = sql.format(table, starttime, endtime)

      println("表名:" + table)
      println("SQL:" + exec_sql)

      val path = target_dir + "/" + yesterday.toString("yyyy/MM/dd")

      val om_log_ask_detail_table = sqlContext.read.format("jdbc")
        .options(Map("driver" -> driver, "url" -> url, "user" -> user, "password" -> password, "dbtable" -> table)).load()
      om_log_ask_detail_table.registerTempTable(table)

      sqlContext.sql(exec_sql).rdd.map(x => x.mkString("|")).saveAsTextFile(path)

    } else if (ask_section == "ask_initial") {
      val begin_date = ini.get(ask_section, "begin_date")
      val end_date = ini.get(ask_section, "end_date")

      var beginDT = DateTime.parse(begin_date)
      val endDT = DateTime.parse(end_date)

      val beginStr = beginDT.toString("yyyyMM")
      val endStr = endDT.toString("yyyyMM")

      //根据时间段划分表名,开始时间,结束时间. 以月为单位划分
      while (beginDT.toString("yyyyMM").toInt <= endDT.toString("yyyyMM").toInt) {
        val table = table_prefix + beginDT.toString("yyyyMM")
        val starttime = if (beginDT.toString("yyyyMM") == beginStr) beginDT.toString(pattern) else beginDT.dayOfMonth.withMinimumValue().secondOfDay().withMinimumValue().toString(pattern)
        val endtime = if (beginDT.toString("yyyyMM") == endStr) endDT.toString(pattern) else beginDT.dayOfMonth.withMaximumValue().secondOfDay().withMaximumValue().toString(pattern)

        val exec_sql = sql.format(table, starttime, endtime)

        println("表名:" + table)
        println("SQL:" + exec_sql)

        val om_log_ask_detail_table = sqlContext.read.format("jdbc")
          .options(Map("driver" -> driver, "url" -> url, "user" -> user, "password" -> password, "dbtable" -> table)).load()
        om_log_ask_detail_table.registerTempTable(table)

        sqlContext.sql(exec_sql).rdd.map(x => x.mkString("|")).saveAsTextFile(tempdir + "/" + DateTime.now().toString("yyyyMMddHHmmss"))
        beginDT = beginDT.plusMonths(1)
      }

      sc.textFile(tempdir + "/*/part-*").saveAsTextFile(target_dir)
      //HadoopOpsUtil.removeOrBackup(tempdir,tempdir)

    } else {
      println("数据导入方式错误,请选择ask或ask_initial")
    }


  }

  /**
    * 加载Ini配置文件
    *
    * @param params
    * @return
    */
  def loadIni(params: Params): Ini = {
    val ini = new Ini()
    ini.load(new File((params.configIni)))
    ini
  }

}

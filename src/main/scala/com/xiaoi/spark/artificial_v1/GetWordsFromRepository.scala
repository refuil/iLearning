package com.xiaoi.spark.artificial_v1

import com.xiaoi.common.HDFSUtil
import org.apache.spark.sql.{DataFrameReader, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

import scala.io.Source

/**
 * created by yang.bai@xiaoi.com
 * 查询知识库中的业务关键词
 */
object GetWordsFromRepository {

  // 公有词库表名
  val PUB_CATEGORY_TABLE_NAME = "pub_kb_wordclass_category"
  val PUB_CLASS_TABLE_NAME = "pub_kb_wordclass"
  val PUB_WORD_TABLE_NAME = "pub_kb_wordclass_word"
  // 私有词库表名
  val PRI_CATEGORY_TABLE_NAME = "kb_wordclass_category"
  val PRI_CLASS_TABLE_NAME = "kb_wordclass"
  val PRI_WORD_TABLE_NAME = "kb_wordclass_word"
  // CONF_FILE KEY
  val PUB_CATEGORY_KEY = "pub_category"
  val PUB_EXCEPT_CLASS_KEY = "pub_except_class"
  val PRI_CATEGORY_KEY = "pri_category"
  val PRI_EXCEPT_CLASS_KEY = "pri_except_class"

  case class Params(
    jdbcUrl: String = "jdbc:mysql://124.74.144.22:8031/test20160630",
    dbUser: String = "test20160630",
    dbPWD: String = "test20160630",
    output: String = "/experiment/baiy/output/step_xiaoi_keywords",
    categoryConfPath: String = "",
    driver:String = "")

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("查询知识库中的业务关键词") {
      opt[String]("output")
        .text("output path")
        .action((x, c) => c.copy(output = x))
      opt[String]("jdbcUrl")
        .text("jdbc url")
        .action((x, c) => c.copy(jdbcUrl = x))
      opt[String]("dbUser")
        .text("database user")
        .action((x, c) => c.copy(dbUser = x))
      opt[String]("dbPWD")
        .text("database password")
        .action((x, c) => c.copy(dbPWD = x))
      opt[String]("categoryConfPath")
        .text("conf file path")
        .action((x, c) => c.copy(categoryConfPath = x))
      opt[String]("driver")
        .text("jdbc driver")
        .action((x, c) => c.copy(driver = x))
    }
    parser.parse(args, defaultParams).map {
      params => run(params)
      //      params => test()
    }.getOrElse {
      sys.exit(1)
    }
  }

  /**
   * 根据分类查询小i知识库词库，并过滤不需要的词类
   * @param database
   * @param categoryTable 分类表
   * @param classTable 词类表
   * @param wordTable 词表
   * @param categorys 所查分类
   * @param filterClass 过滤词类
   * @return
   */
  def getXiaoiWords(database: DataFrameReader, categoryTable: String, classTable: String, wordTable: String,
    categorys: String, filterClass: String): List[String] = {
    //pub_kb_wordclass_category
    val categoryT = database.option("dbtable", categoryTable).load()
    val firstCategoryIDs = categoryT
      .filter("NAME in ('%s') and PARENT_CATEGORY_ID is null".format(categorys.split(",").mkString("','")))
      .map(_.getAs[String]("CATEGORY_ID"))
      .collect()
    var categoryIDs = firstCategoryIDs
    var tmpCategoryIds = firstCategoryIDs
    while(tmpCategoryIds.length != 0){
      val secCategoiryIDs = categoryT
        .filter("PARENT_CATEGORY_ID in ('%s')".format(tmpCategoryIds.mkString("','")))
        .map(_.getAs[String]("CATEGORY_ID"))
        .collect()
      tmpCategoryIds = secCategoiryIDs
      categoryIDs = categoryIDs ++ secCategoiryIDs
    }

    val classT = database.option("dbtable", classTable).load()
    val filterClassIDs = classT
      .filter("NAME in ('%s')".format(filterClass.replaceAll(",", "','")))
      .map(_.getAs[String]("ID"))
      .collect()
    val classIDs = classT
      .filter("CATEGORY_ID in ('%s')".format(categoryIDs.mkString("','")))
      .map(_.getAs[String]("ID"))
      .filter(x => !filterClassIDs.contains(x))
      .collect()

    val pubWord = database.option("dbtable", wordTable).load()
    val pubWordName = pubWord
      .filter("WORDCLASS_ID in ('%s')".format(classIDs.mkString("','")))
      .map(_.getAs[String]("NAME"))
      .collect()
      .toList
    pubWordName
  }

  def getCategoryConf(confFilePath: String): Map[String, String] = {
    val confFile = Source.fromFile(confFilePath)
    val conf = confFile
      .getLines()
      .map(x => {
        val lineSplit = x.split("=", -1)
        (lineSplit(0), lineSplit(1))
      })
      .toMap
    confFile.close()
    conf
  }

  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName("get words from repository")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    val url = params.jdbcUrl
    val user = params.dbUser
    val pwd = params.dbPWD
    val confFilePath = params.categoryConfPath
    val output = params.output
    val driver = params.driver

    HDFSUtil.removeOrBackup(output, output)

    val database = sqlContext.read.format("jdbc")
      .options(Map("driver" -> driver, "url" -> url, "user" -> user, "password" -> pwd))

    val categoryConf = getCategoryConf(confFilePath)
    //pub_kb_wordclass_category
    val pubCategorys = categoryConf(PUB_CATEGORY_KEY)
    val pubFilterClass = categoryConf(PUB_EXCEPT_CLASS_KEY)
    val pubKeyWords = getXiaoiWords(database,
      PUB_CATEGORY_TABLE_NAME, PUB_CLASS_TABLE_NAME, PUB_WORD_TABLE_NAME,
      pubCategorys, pubFilterClass)

    //kb_wordclass_category
    val priCategoryTable = "kb_wordclass_category"
    val priCategorys = categoryConf(PRI_CATEGORY_KEY)
    val priFilterClass = categoryConf(PRI_EXCEPT_CLASS_KEY)
    val priKeyWords = getXiaoiWords(database,
      PRI_CATEGORY_TABLE_NAME, PRI_CLASS_TABLE_NAME, PRI_WORD_TABLE_NAME,
      priCategorys, priFilterClass)

    sc.makeRDD(pubKeyWords.union(priKeyWords).map(x => (x, 1.0)))
      .filter(x => x._1.length >= 2)
      .map(x => x._1 + "\t" + x._2)
      .saveAsTextFile(output)

  }
}

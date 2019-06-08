package com.xiaoi.common

import java.sql.{Connection, DriverManager}
import java.util.Properties

import scala.collection.mutable.HashMap

/**
  * Created by Jody.zhang on 2016/11/16.
  */
object DBUtil  extends Serializable{

  def getConnection(url:String,user:String,pwd:String,driver:String):Connection={
    Class.forName(driver)
    val conn = DriverManager.getConnection(url,user,pwd)
    conn
  }

  def getConnection(url:String,user:String,pwd:String):Connection={
    Class.forName(ConfigurationManager.getString(Constants.JDBC_DRIVER))
    val conn = DriverManager.getConnection(url,user,pwd)
    conn
  }

  //插入、更新数据
  def update(url: String, userName: String, pwd: String, dataDF: DataFrame, table: String): Boolean = {
    val prop = new java.util.Properties
    prop.setProperty("user", userName)
    prop.setProperty("password", pwd)
    dataDF.write.mode(SaveMode.Append).jdbc(url, table, prop)
    //dataDF.write.mode(SaveMode.Overwrite).jdbc(url,table,prop)
    true
  }

  //重写表数据
  def rewrite(url: String, userName: String, pwd: String, dataDF: DataFrame, table: String): Boolean = {
    val prop = new java.util.Properties
    prop.setProperty("user", userName)
    prop.setProperty("password", pwd)
    dataDF.write.mode(SaveMode.Overwrite).jdbc(url, table, prop)
    true
  }

  //查询数据
  def query(url: String, userName: String, pwd: String, sqlContext: SQLContext, table: String): DataFrame = {
    val prop = new java.util.Properties
    prop.setProperty("user", userName)
    prop.setProperty("password", pwd)
    sqlContext.read.jdbc(url, table, prop)
  }

  def query(url: String, userName: String, pwd: String, sqlContext: SQLContext, table: String,dbDriver: String): DataFrame = {
    val prop = new java.util.Properties
    prop.setProperty("user", userName)
    prop.setProperty("password", pwd)
    prop.setProperty("driver",dbDriver)
    sqlContext.read.jdbc(url, table, prop)
  }

  //查询小量数据，返回字典
  def query_opts(host: String, db: String, user: String, pwd: String, port: Int = 3306): HashMap[String, String] = {
    val sql = "select k,v from config"
    val conn = DriverManager.getConnection(s"jdbc:mysql://${host}:${port}/${db}", user, pwd)

    val result = conn.prepareStatement(sql).executeQuery()
    //把权重字符串，转换成map
    var config_map: HashMap[String, String] = HashMap()
    while (result.next()) {
      val k = result.getString(1)
      val v = result.getString(2)
      config_map += (k -> v)
    }
    conn.close()
    config_map
  }

  //查询小量数据，返回字典
  def query4small(url: String, user: String, pwd: String, table: String, key: String,value: String):
        HashMap[String, String] = {
    val sql = s"select ${key},${value} from ${table}"
    val conn = DriverManager.getConnection(url, user, pwd)

    val result = conn.prepareStatement(sql).executeQuery()
    //把权重字符串，转换成map
    var config_map: HashMap[String, String] = HashMap()
    while (result.next()) {
      val k = result.getString(1)
      val v = result.getString(2)
      config_map += (k -> v)
    }
    conn.close()
    config_map
  }

  //更新小量数据，返回字典
  def update_opts(sql: String, host: String, db: String, user: String, pwd: String, port: Int = 3306): Boolean = {
    val conn = DriverManager.getConnection(s"jdbc:mysql://${host}:${port}/${db}", user, pwd)
    val result = conn.prepareStatement(sql).executeUpdate()
    conn.close()
    if (result > 0) true else false
  }

  //更新小量数据，返回字典
  def update4small(sql: String, url: String, user: String, pwd: String): Boolean = {
    val conn = DriverManager.getConnection(url, user, pwd)
    val result = conn.prepareStatement(sql).executeUpdate()
    conn.close()
    if (result > 0) true else false
  }

  /**
    * 数据库删除数据
    * @param sql
    * @param props
    * @return
    */
  def deleteBy(sql: String, props: Properties): Boolean = {
    val driver = props.getProperty("driver")
    val url = props.getProperty("url")
    val username = props.getProperty("username")
    val password = props.getProperty("password")

    var conn: Connection = null
    try {
      Class.forName(driver)
      conn = DriverManager.getConnection(url, username, password)
      conn.createStatement().execute(sql)
    } catch {
      case e: Exception => e.printStackTrace()
        return false
    } finally {
      conn.close()
    }

  }

  /**
    * Wrap the method of deleteBy
    * @param url
    * @param userName
    * @param pwd
    * @param sql
    * @return
    */
  def del(url: String, userName: String, pwd: String, sql: String): Boolean = {
    val prop = new java.util.Properties
    prop.setProperty("driver", ConfigurationManager.getString(Constants.JDBC_DRIVER))
    prop.setProperty("url", url)
    prop.setProperty("username", userName)
    prop.setProperty("password", pwd)
    deleteBy(sql, prop)
  }

  /**
    * Test for the object
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val url = "jdbc:mysql://172.16.9.48:3306/test20160630"
    val user = "test20160630"
    val pwd = "test20160630"
    val conn = getConnection(url, user, pwd)
    try {
      val statement = conn.createStatement()
      val resultSet = statement.executeQuery("select * from kb_wordclass_word limit 10")
      while (resultSet.next()) {
        val word_id = resultSet.getString("word_id")
        val name = resultSet.getString("name")
        println("word_id, name = " + word_id + ", " + name)
      }
    } finally {
      if (conn != null) conn.close()
    }
  }
}

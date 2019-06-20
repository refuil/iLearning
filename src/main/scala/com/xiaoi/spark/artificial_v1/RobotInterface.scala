package com.xiaoi.spark.artificial_v1

import scala.xml.XML
import scalaj.http.{Http, HttpResponse}

/**
  * RobotInterface
  *
  * Created by ligz on 16/8/18.
  */
object RobotInterface {


  /**
    * 调用机器人问答接口,返回是否能够回答
    *
    * @param question  问题
    * @param robotUri 机器人问答url
    * @return
    */
  def hasAnswer(question: String, robotUri: String, toReplace: String, ansTypes: Set[Int] = Set(1)): Boolean = {
    // 使用HTTP调用机器人管理接口,获取响应进行解析
//    val response: HttpResponse[String] = Http(robotUri)
//      .params(Map("userId" -> "111", "question" -> question, "platform" -> "web")).asString
    val realUrl = robotUri.replace(toReplace, question.replaceAll("\\s+", ","))
//    println(realUrl)
    val response: HttpResponse[String] = Http(realUrl).timeout(connTimeoutMs = 3000, readTimeoutMs = 5000).asString
//    println(response.body)
    val body = XML.loadString(response.body)
    val code = response.code
    val ansType = (body \ "Type").text.toInt
    val content = (body \ "Content").text

    val contentValid = !(content isEmpty)
    val hasAns = (code == 200) && (ansTypes.contains(ansType) && contentValid)
    return hasAns
  }


  /**
    * 调用机器人问答接口, 返回回答类型和回答内容(前50个字符)
    *
    * @param question
    * @param robotUri
    * @return
    */
  def getAnswer(question: String, robotUri: String, toReplace: String): (Int, String) = {
    // 使用HTTP调用机器人管理接口,获取响应进行解析
//    val response: HttpResponse[String] = Http(robotUri)
//      .params(Map("userId" -> "111", "question" -> question, "platform" -> "web")).asString
    val realUrl = robotUri.replace(toReplace, question.replaceAll("\\s+", ","))
    val response: HttpResponse[String] = Http(realUrl).asString
    val body = XML.loadString(response.body)
//    val code = response.code
    val ansType = (body \ "Type").text.toInt
    val content = (body \ "Content").text
    (ansType, if (content.isEmpty) "" else content.take(50))
  }

}

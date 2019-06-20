package com.xiaoi.spark.artificial_v1

import java.io.{File, PrintWriter}
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger

import scopt.OptionParser

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * MultiThreadFilterQuestionLocal
  * 本地多线程调用机器人问答接口
  *
  * Created by ligz on 16/7/8.
  */
object MultiThreadFilterQuestionLocal {

  var executors: ExecutorService = _
  var ansTypes: Set[Int] = _
  var sleepInMills: Int = _
  var printInterval: Int = _
  var numThreads: Int = _
  var toReplace: String = _
  var waitSecs: Int = _


  /**
    * 多线程调用机器人问答接口,返回不能回答的问题列表
    *
    * @param querys
    * @param robotUri
    * @return
    */
  def multiThread(querys: List[String], robotUri: String): List[(String, Boolean)] = {

    println(s"线程数:  $numThreads")
    executors = Executors.newFixedThreadPool(numThreads)
    val counter = new AtomicInteger(0)
    val start = System.currentTimeMillis()


    val ecs = new ExecutorCompletionService[(String, Boolean)](executors)

    val callerList = querys.map(q => new Callable[(String, Boolean)] {
      override def call() = {
        q -> RobotInterface.hasAnswer(q, robotUri, toReplace, ansTypes)
      }
    })

    val futureSet = mutable.Set[Future[(String, Boolean)]]()
    callerList.map { caller =>
      val f = ecs.submit(caller)
      futureSet.add(f)
      f
    }

    val answers = ListBuffer[(String, Boolean)]()
    while (!futureSet.isEmpty) {
      val f = ecs.take()
      futureSet.remove(f)
      // 打印执行进度
      val cnt = counter.incrementAndGet()
      if (cnt % printInterval == 0) {
        val usedTime = getUsedTime(start)
        println(s"已完成行数: $cnt, 耗时:${usedTime / 1000}s")
      }
      answers += f.get()
    }
    executors.shutdown()
    answers.toList


//
//
//    val tasks: List[FutureTask[(String, Boolean)]] = querys.map(qa => {
//      // 创建Task
//      val futureTask: FutureTask[(String, Boolean)] = new FutureTask(
//        new Callable[(String, Boolean)] {
//          override def call(): (String, Boolean) = {
//            val q = qa.split("\\|")(0)
//            qa -> RobotInterface.hasAnswer(q, robotUri, toReplace, ansTypes)
//          }
//        }
//      )
//      // 使用ExecutorService来调度线程执行task
//      executors.execute(futureTask)
//      futureTask
//    })
//
//    val results = tasks.map {
//      f =>
//        val task = f
//        try {
//          // 获取Task的结果
//          val result = task.get(waitSecs, TimeUnit.SECONDS)
//
//          // 打印执行进度
//          val cnt = counter.incrementAndGet()
//          if (cnt % printInterval == 0) {
//            val usedTime = getUsedTime(start)
//            println(s"已完成行数: $cnt, 耗时:${usedTime / 1000}s")
//          }
//          result
//        } catch {
//          case e: Exception => {
//            e.printStackTrace()
//            ("", false)
//          }
//        }
//    }
//    executors.shutdown()
//    results
  }


  def singleThread(querys: List[String], robotUri: String): List[(String, Boolean)] = {
    var i = 0
    val start = System.currentTimeMillis()

    def getAnswerStatus(q: String) = {
      val hasAns = RobotInterface.hasAnswer(q, robotUri, toReplace, ansTypes)
      TimeUnit.MILLISECONDS.sleep(sleepInMills)
      i += 1
      if (i % printInterval == 0) {
        val usedTime = getUsedTime(start)
        println(s"已完成行数: $i, 耗时:${usedTime / 1000}s")
      }
      (q, hasAns)
    }

    querys.map(getAnswerStatus)
  }


  def run(params: Params): Unit = {

    val inputPath = params.inputPath
    val outputPath = params.outputPath
    val robotUri = params.robotUri
    ansTypes = params.answerTypes.split(",").map(_.trim.toInt).toSet
    sleepInMills = params.sleepMs
    waitSecs = params.waitSecs
    printInterval = params.printInter
    numThreads = params.numThreads
    toReplace = params.toReplace

    // 读取输入问题列表
    val lines = Source.fromFile(inputPath).getLines().map(_.split("\\|")(0)).toList
    val writer = new PrintWriter(new File(outputPath))

    val results = if (params.isSingleThread) singleThread(lines, robotUri)
    else multiThread(lines, robotUri)

    results.filter(!_._2).foreach(x => writer.write(x._1 + "\n"))
    writer.close()
    println("过滤完成!")

  }

  // 计算总耗时
  def getUsedTime(start: Long): Long = {
    System.currentTimeMillis() - start
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("Question Filter Locally") {
      head("question filter parameters")
      opt[String]("inputPath")
        .text("输入文件路径")
        .action((x, c) => c.copy(inputPath = x))
      opt[String]("robotUri")
        .text("机器人问答接口URI")
        .action((x, c) => c.copy(robotUri = x))
      opt[Int]("numThreads")
        .text("线程个数")
        .action((x, c) => c.copy(numThreads = x))
      opt[String]("ansTypes")
        .text("被认作已回答的Type,逗号分隔,默认:1,可选:1,11")
        .action((x, c) => c.copy(answerTypes = x))
      opt[String]("outputPath")
        .text("输出文件路径")
        .action((x, c) => c.copy(outputPath = x))
      opt[Boolean]("isSingleThread")
        .text("是否使用单线程,默认为:false")
        .action((x, c) => c.copy(isSingleThread = x))
      opt[Int]("sleepMs")
        .text("线程间隔时间")
        .action((x, c) => c.copy(sleepMs = x))
      opt[Int]("waitSecs")
        .text("等待结果返回时间")
        .action((x, c) => c.copy(waitSecs = x))
      opt[Int]("printInter")
        .text("处理多少个问句后打印信息")
        .action((x, c) => c.copy(printInter = x))
      opt[String]("toReplace")
        .text("需要替换的url中的问题占位字符串")
        .action((x, c) => c.copy(toReplace = x))

      checkConfig { _ => success }
    }
    parser.parse(args, defaultParams).map { params => run(params) }
      .getOrElse(sys.exit(1))

  }

  case class Params(
                     inputPath: String = "",
                     robotUri: String = "",
                     numThreads: Int = 10,
                     answerTypes: String = "1",
                     outputPath: String = "",
                     isSingleThread: Boolean = false,
                     sleepMs: Int = 1,
                     waitSecs: Int = 1,
                     printInter: Int = 200,
                     toReplace: String = "[问题]"
                   )

}

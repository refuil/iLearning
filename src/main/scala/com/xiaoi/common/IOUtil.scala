package com.xiaoi.common

object IOUtil {
  /**
    * java接口写本地磁盘
    * @param result
    */
  def writeToLocalFile(result: Array[String], file_path: String): Unit ={
    import java.io._
    try {
      val out = new PrintWriter(new File(file_path))
      for (line <- result) {
        out.write(line)
        out.write("\n")
      }
      out.close()
    } catch {
      case ex: IOException => println("Write File Error")
    }
  }

}

package com.xiaoi.common

import com.github.nscala_time.time.Imports._
import com.xiaoi.common.Segment.SegmentUtil
import com.xiaoi.constant.Constants
import org.apache.log4j.lf5.viewer.configure.ConfigurationManager
import org.apache.spark.sql.functions._

/** ***********************************************************************
  * Function:
  * Author: DRUNK
  * mail: shuangfu.zhang@xiaoi.com
  * Created Time: Wed 16 Dec 2015 10:38:34 AM CST
  * **************************************************************************/

//自定义dataFrame函数
object UDF extends Serializable {
  val stopWords = ConfigurationManager.getString(Constants.STOP_WORDS).split("\\|", -1).toArray
  var segmentUtil = new SegmentUtil(stopWords)
  //获取UUID
  val getUUID = udf { () => java.util.UUID.randomUUID.toString.replace("-", "") }

  //获取静态字符串
  val addColumn = udf { (arg: String) => arg }
  val addColumn_d = udf { (arg: Double) => arg }

  override def hashCode(): Int = super.hashCode()

  //分词
  val segment = udf { (arg: String) =>
    segmentUtil.segment(arg).mkString(" ")
  }

  //字符串拼接
  val concat = udf { (left: String, right: String) =>
    left + right
  }

  //获取字符串长度
  val len = udf { (arg: String) =>
    arg.length
  }

  //时间格式化
  val dateFormat = udf { (dateStr: String, srcFormatStr: String, targetFormatStr: String) =>
    DateTimeFormat.forPattern(srcFormatStr).parseDateTime(dateStr).toString(targetFormatStr)
  }

  //判断字符词,是否包含英文单词
  val isContainEnglish = udf { (text: String) =>
    val flag = text.matches(""".*[A-Za-z]{2,}.*""")
    flag

  }

  //判断是否包含汉字
  val isContainChinese = udf { (text: String) =>
    //[\u4e00-\u9fa5]汉字
    val flag = text.matches(""".*[\u4e00-\u9fa5].*""")
    flag
  }

  //判断是否包含数字
  val isContainNumber = udf { (text: String) =>
    val flag = text.matches(""".*[0-9].*""")
    flag
  }

  //判断是否为英文句子
  val isEnglishSentence = udf { (input: String) =>
    val flag = input.matches(""".*([^\\pPa-zA-Z,.?!]).*""")
    !flag
  }

  //获取时间差
  val getDiffDateTime = udf { (beginDateTime: String, endDateTime: String) =>
    DateUtil.dateTimeDiff(beginDateTime, endDateTime)
  }

  //获取日期
  val getDate = udf { (date: String) =>
    DateUtil.getDate(date)
  }

}

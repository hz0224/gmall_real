package util

import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by Liu HangZhou on 2020/03/29
  * desc: 日期工具类
  */
object DateUtil {

  def main(args: Array[String]): Unit = {
    println(getCurrentDate())
  }

  private val formatDay = new SimpleDateFormat("yyyy-MM-dd")
  private val formatHour = new SimpleDateFormat("yyyy-MM-dd HH")


  //获取当前日期
  def getCurrentDate()={
    val date = new Date()
    val currentDay = formatDay.format(date)
    currentDay
  }


  //将时间戳转换为日期
  def getDate(ts: Long)={
    val date = new Date(ts)
    formatHour.format(date)
  }

}

package util

import java.util.Properties

/**
  * Created by Liu HangZhou on 2020/03/27
  * desc: 获取配置文件值工具类.
  */

object PropertiesUtil {

  def main(args: Array[String]): Unit = {
    val properties: Properties = PropertiesUtil.load("config.properties")
    println(properties.getProperty("redis.password"))
  }

  def load(propertieName:String): Properties ={
    val prop=new Properties()
    val in = PropertiesUtil.getClass.getClassLoader.getResourceAsStream(propertieName)
    prop.load(in)
    prop
  }

}


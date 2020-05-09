package app

import bean.StartupLog
import com.alibaba.fastjson.JSON
import constant.GmallConstant
import org.apache.hadoop.conf.Configuration
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import util.{DateUtil, MyKafkaUtil, MyRedis}
import org.apache.phoenix.spark._


/**
  * Created by Liu HangZhou on 2020/03/29
  * desc: 日活 (daily active user)
  */

object DauApp {

  def runTask(sc: SparkContext, args: Array[String]): Unit = {
    val ssc = new StreamingContext(sc,Seconds(5))
    sc.setLogLevel("WARN")

    val inputDStream = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP,ssc)
    //注意：inputDStream中的 ConsumerRecord对象是不可以序列化的,直接使用如inputDStream.print() 会报错.
    val startupLogStringDStream = inputDStream.map{record =>
        record.value()
    }


    //统计日活
    //将json转换成case class，补充两个日期
    val startupLogDStream = startupLogStringDStream.map {startupLogString=>
        //val startupLog:  = JSON.parseObject(startupLog, classOf[StartupLog])
        val startupLog = JSON.parseObject(startupLogString, classOf[StartupLog])
        //date: 2020-03-29 11
        val date = DateUtil.getDate(startupLog.ts)
        val day = date.split(" ")(0)
        val hour = date.split(" ")(1)
        startupLog.logDate = day
        startupLog.logHour = hour
        startupLog
    }

    //去重(以mid为标准去重)   3个步骤：1 完成批次内的去重  2 完成批次之间的去重 (查询清单过滤)  3 写入外部存储

    //1 完成批次内的去重
    val mid2StartupLogDStream = startupLogDStream.map {startupLog =>
        (startupLog.mid, startupLog)
    }.groupByKey()

    val filterStartupLogDStream = mid2StartupLogDStream.flatMap{
      case (mid, startupLogIter) =>
       startupLogIter.take(1)
    }


    //2 完成批次之间的去重 (查询清单过滤)
    val realFilterStartupLogDStream = filterStartupLogDStream.transform {startupLogRDD =>
        //查询redis获取清单
        println("过滤前:" + startupLogRDD.count())
        val jedis = MyRedis.getClient()
        val key = "dau:" + DateUtil.getCurrentDate()
        val mids = jedis.smembers(key)
        val midsBC = ssc.sparkContext.broadcast(mids)
        jedis.close()

        //过滤    true
        val filterRDD = startupLogRDD.filter {startupLog =>
            !midsBC.value.contains(startupLog.mid)
        }
        println("过滤后: " + filterRDD.count())
        filterRDD
    }

    realFilterStartupLogDStream.cache()

    //3 写mid到 redis清单中
    realFilterStartupLogDStream.foreachRDD{startuplogRDD=>
        startuplogRDD.foreachPartition{startupLogIter=>
            val jedis = MyRedis.getClient()
          for(startupLog <- startupLogIter){
             val key = "dau:" + startupLog.logDate
             jedis.sadd(key,startupLog.mid)
          }
            jedis.close()
        }
    }



    //写入到hbase中.
    realFilterStartupLogDStream.foreachRDD{rdd=>

      rdd.saveToPhoenix(
        "gmall_dau",
        Seq("MID","UID","APP_ID","AREA","OS","CH","LOG_TYPE","VS","LOG_DATE","LOG_HOUR","TS"),
        new Configuration,
        Some("hangzhou1-yun,hangzhou2-yun,hangzhou3-yun:2181"))
    }//无论这里的表名定义的是小写或是大写,都会转换成大写.所以创建表时表名需要使用大写.

    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
      val currentDate = DateUtil.getCurrentDate()
      val conf = new SparkConf().setAppName("dau_app_" + currentDate).setMaster("local[*]")
      val sc = new SparkContext(conf)
      runTask(sc,args)
  }
}

package app

import bean.{AlertInfo, EventInfo}
import com.alibaba.fastjson.JSON
import constant.GmallConstant
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import util.{DateUtil, MyKafkaUtil,MyEsUtil}
import java.util

import scala.util.control.Breaks._

object AlertApp {


  def runTask(sc: SparkContext, args: Array[String]): Unit = {

    val ssc = new StreamingContext(sc,Seconds(5))
    ssc.sparkContext.setLogLevel("WARN")

    val inputDStream = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_EVENT,ssc)
    val EventLogDStream = inputDStream.map { record =>
      val eventLogJson = record.value()
      val eventInfo = JSON.parseObject(eventLogJson, classOf[EventInfo])
      eventInfo
    }

    //窗口函数 window   5分钟内的数据聚合到一块
    val windowDStream = EventLogDStream.window(Minutes(5),Seconds(5))
    //按照mid进行分组
    val mid2EventInfo = windowDStream.map{eventInfo=>(eventInfo.mid,eventInfo)}.groupByKey()
    //处理逻辑
    val checkedDStream = mid2EventInfo.map { case (mid, eventInfoIter) =>
      //使用过的账号
      val uids = new util.HashSet[String]()
      //领的那些商品的优惠卷
      val itemIds = new util.HashSet[String]()
      //该mid进行过的所有事件，不去重
      val eventList = new util.ArrayList[String]()

      //是否浏览商品  默认是没有
      var isClickItem = false
      breakable {
        for (eventInfo <- eventInfoIter) {
          eventList.add(eventInfo.evid)
          //三次及以上用不同账号登录并领取优惠劵
          if ("coupon".equals(eventInfo.evid)) {
            uids.add(eventInfo.uid)
            itemIds.add(eventInfo.itemid)
          }

          if ("clickItem".equals(eventInfo.evid)) {
            isClickItem = true
            break()
          }
        }
      }
      (uids.size() >= 3 && !isClickItem, AlertInfo(mid, uids, itemIds, eventList, System.currentTimeMillis())) //(是否符合预警的条件,预警日志对象)
    }

    val filterAlterDStream = checkedDStream.filter{case (isAlter,alertInfo)=>isAlter}.map{case (isAlter,alertInfo)=>alertInfo}

    //保存到ES中.
    filterAlterDStream.foreachRDD{rdd=>
      rdd.foreachPartition{ alertItr=>
        val list: List[AlertInfo] = alertItr.toList
        //提取主键  // mid + 分钟 组合成主键 同时 也利用主键进行去重
        val alterListWithId: List[(String, AlertInfo)] = list.map(alertInfo=> (alertInfo.mid+"_"+ alertInfo.ts/1000/60   ,alertInfo))
        //批量保存
        MyEsUtil.indexBulk(GmallConstant.ES_INDEX_ALERT,alterListWithId)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {

    val currentDate = DateUtil.getCurrentDate()
    val conf = new SparkConf().setAppName("alert_app_" + currentDate).setMaster("local[*]")
    val sc = new SparkContext(conf)
    runTask(sc,args)
  }

}

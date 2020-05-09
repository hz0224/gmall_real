package app

import bean.OrderInfo
import constant.GmallConstant
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import util.{DateUtil, MyKafkaUtil}
import com.alibaba.fastjson.JSON
import org.apache.hadoop.conf.Configuration
import org.apache.phoenix.spark._
/**
  * Created by Liu HangZhou on 2020/04/01
  * desc: 处理订单表,得到实时销售额
  */
object OrderApp {

  def runTask(sc: SparkContext, args: Array[String]): Unit = {
    val ssc = new StreamingContext(sc,Seconds(5))
    ssc.sparkContext.setLogLevel("WARN")
    val inputDStream = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER,ssc)
    val OrderJsonDStream = inputDStream.map { record =>
      record.value()
    }

    //1 加时间  2 电话号码脱敏
    val orderInfoDstream = OrderJsonDStream.map { orderInfoJson =>
      val orderInfoObj = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
      //2020-04-01 06:19:27
      val create_time = orderInfoObj.create_time
      val date = create_time.substring(0, 13)
      orderInfoObj.create_date = date.split(" ")(0)
      orderInfoObj.create_hour = date.split(" ")(1)

      val tuple = orderInfoObj.consignee_tel.splitAt(4)
      orderInfoObj.consignee_tel = tuple._1 + "*******"
      orderInfoObj
    }
    //保存到hbase
    orderInfoDstream.foreachRDD{rdd=>
      rdd.saveToPhoenix("gmall_order_info",Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        new Configuration,Some("hangzhou1-yun,hangzhou2-yun,hangzhou3-yun:2181")
      )
    }


    ssc.start()
    ssc.awaitTermination()

  }

  def main(args: Array[String]): Unit = {

    val currentDate = DateUtil.getCurrentDate()
    val conf = new SparkConf().setAppName("order_app_" + currentDate).setMaster("local[*]")
    val sc = new SparkContext(conf)
    runTask(sc,args)

  }

}

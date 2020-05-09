package app

import bean.{OrderDetail, OrderInfo}
import com.alibaba.fastjson.JSON
import constant.GmallConstant
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import util.{DateUtil, MyKafkaUtil}

/**
  * Created by Liu HangZhou on 2020/04/11
  * desc: 灵活分析
  */
object SaleApp {

  def runTask(sc: SparkContext, args: Array[String]): Unit = {

    val ssc = new StreamingContext(sc,Seconds(5))

    val userInfoInputDStream = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_USER_INFO,ssc)
    val orderInputDStream = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER,ssc)
    val orderDetailInputDStream = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER_DETAIL,ssc)

    // order_info简单处理      1 加时间  2 电话号码脱敏
    val orderInfoDstream = orderInputDStream.map { record =>
      val orderInfoJson = record.value()
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


    //转换为case class
    val orderDetailDStream = orderDetailInputDStream.map { record =>
      val orderDetailJson = record.value()
      JSON.parseObject(orderDetailJson, classOf[OrderDetail])
    }

    //变换成kv结构
    val orderId2OrderInfoDStream = orderInfoDstream.map{orderInfo=>(orderInfo.id,orderInfo)}
    val orderId2OrderDetail = orderDetailDStream.map{orderDetail=>(orderDetail.order_id,orderDetail)}

    //全外连接
    val orderId2OrderInfo_orderDetailDSream = orderId2OrderInfoDStream.fullOuterJoin(orderId2OrderDetail)

    orderId2OrderInfo_orderDetailDSream.map{case (orderId,(orderInfoOpt,orderDetailOpt))=>



    }





  }

  def main(args: Array[String]): Unit = {

      val currentDate = DateUtil.getCurrentDate()
      val conf = new SparkConf().setAppName("sale_app_" + currentDate).setMaster("local[*]")
      val sc = new SparkContext(conf)
      runTask(sc,args)

  }
}

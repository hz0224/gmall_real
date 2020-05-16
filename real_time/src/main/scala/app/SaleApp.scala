package app

import bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.alibaba.fastjson.JSON
import constant.GmallConstant
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.Serialization
import util.{DateUtil, MyEsUtil, MyKafkaUtil, MyRedis}

import scala.collection.mutable.ArrayBuffer

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

    userInfoInputDStream.print()

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


    //处理    订单和订单详情是一对多的关系,因此order_info只会来一次，order_detail会来多次
    val saleDetailDStream = orderId2OrderInfo_orderDetailDSream.flatMap{case (orderId,(orderInfoOpt,orderDetailOpt))=>
      //1 如果order_info不为none
      //① 如果从表order_detail也不为none，说明关联成功，则进行合并成宽表。
      //② 把自己写入缓存
      //③ 查询order_detail表缓存   可能有提前来的 order_detail
      val saleDetails = new ArrayBuffer[SaleDetail]()
      implicit val formats = org.json4s.DefaultFormats
      val jedis = MyRedis.getClient()

      if(orderInfoOpt.isDefined){
        val orderInfo = orderInfoOpt.get
        if(orderDetailOpt.isDefined){
          val orderDetail = orderDetailOpt.get
          saleDetails += new SaleDetail(orderInfo,orderDetail)
        }

        //写order_info缓存
        //type: String  key: order_info:orderId   value: order_info_json
        val order_info_key = "order_info:" + orderInfo.id
        val orderInfoJson = Serialization.write(orderInfo)
        jedis.setex(order_info_key,60 * 60 ,orderInfoJson)

        //查order_detail缓存
        //key: order_detail:orderId
        val order_detail_key = "order_detail:" + orderInfo.id
        val orderDetails = jedis.smembers(order_detail_key)
        import scala.collection.JavaConversions._
        for(orderDetailJson <- orderDetails){
          val orderDetail = JSON.parseObject(orderDetailJson,classOf[OrderDetail])
          saleDetails += new SaleDetail(orderInfo,orderDetail)
        }
      }else if(!orderInfoOpt.isDefined){
        //2 如果order_info为none(说明它还没有来,他可能在下一批来并且只会来一次)，那么order_detail一定不为none.
        //① 把order_detail自己写入缓存
        //② 查询order_info表缓存

        //写 order_detail缓存
        val orderDetail = orderDetailOpt.get
        val orderDetailJson = Serialization.write(orderDetail)
        val order_detail_key = "order_detail:" + orderDetail.order_id
        jedis.sadd(order_detail_key,orderDetailJson)
        //jedis.expire(order_detail_key,60 * 60)

        //查order_info缓存
        val orderInfoJson = jedis.get("order_info:" + orderDetail.order_id)
        if(orderInfoJson != null && orderInfoJson.length >0){
          val orderInfo = JSON.parseObject(orderInfoJson,classOf[OrderInfo])
          saleDetails += new SaleDetail(orderInfo,orderDetail)
        }
      }////无论是哪种情况都要进行查询缓存和将自己写入缓存,只有这样才能保证数据完整。

      jedis.close()
      saleDetails
    }

    //userInfo数据写入到redis中.
    userInfoInputDStream.map{userInfoRecord=>
      userInfoRecord.value()
    }.foreachRDD{rdd=>
      rdd.foreachPartition{userInfoJsonIter=>
        val jedis = MyRedis.getClient()
        for(userInfoJson <- userInfoJsonIter){
          val userInfo = JSON.parseObject(userInfoJson,classOf[UserInfo])
          jedis.hset("user_info_dim","userId:" + userInfo.id ,userInfoJson)
        }
        jedis.close()
      }
    }

    //关联用户维表
    val saleFullDetailDStream = saleDetailDStream.mapPartitions { saleDetailIter =>
      val details = new ArrayBuffer[SaleDetail]()
      val jedis = MyRedis.getClient()

      for (saleDetail <- saleDetailIter) {
        val user_id = saleDetail.user_id
        //查询redis
        val userJson = jedis.hget("user_info_dim", "userId:" + user_id)
        if (userJson != null) {
          val userInfo = JSON.parseObject(userJson, classOf[UserInfo])
          saleDetail.mergeUserInfo(userInfo)
        }
        details += saleDetail
      }
      details.toIterator
    }


    saleFullDetailDStream.foreachRDD{rdd=>
      rdd.foreachPartition{saleDetailIter=>
        val saleDetails = saleDetailIter.map(saleDetail=>(saleDetail.order_detail_id,saleDetail)).toList
        MyEsUtil.indexBulk(GmallConstant.ES_INDEX_SALE,saleDetails)
      }
    }


    saleFullDetailDStream.print()


    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {

      val currentDate = DateUtil.getCurrentDate()
      val conf = new SparkConf().setAppName("sale_app_" + currentDate).setMaster("local[*]")
      val sc = new SparkContext(conf)
      runTask(sc,args)


  }
}

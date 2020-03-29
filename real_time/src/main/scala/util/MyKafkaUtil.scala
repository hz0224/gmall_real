package util

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
  * Created by Liu HangZhou on 2020/03/28
  * desc: kafka工具类
  */

object MyKafkaUtil {
  private val properties: Properties = PropertiesUtil.load("config.properties")
  val broker_list = properties.getProperty("kafka.broker.list")

  // kafka消费者配置
  val kafkaParam = Map(
    "bootstrap.servers" -> broker_list,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "gmall_consumer_group",
    "auto.offset.reset" -> "latest",
    //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
    //如果是false，会需要手动维护kafka偏移量,较为复杂.
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )


  def getKafkaStream(topic: String,ssc:StreamingContext): InputDStream[ConsumerRecord[String,String]]={
    val dStream = KafkaUtils.createDirectStream[String,String](ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String,String](Array(topic),
        kafkaParam))
    dStream
  }
}


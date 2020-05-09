package util


import redis.clients.jedis.Jedis
import scala.collection.JavaConversions._

//redis工具类
object MyRedis {

  private val properties = PropertiesUtil.load("config.properties")
  private val host = properties.getProperty("redis.host")
  private val port = properties.getProperty("redis.port").toInt
  private val password = properties.getProperty("redis.password")
  private val timeout = properties.getProperty("redis.timeout").toInt
  private var jedis = new Jedis(host,port,timeout)
  jedis.auth(password)

  //获取连接
  def getClient(): Jedis={
    if(jedis.isConnected){
      jedis
    }else{
      val newJedis = new Jedis(host,port,timeout)
      newJedis.auth(password)
      newJedis
    }
  }



  def main(args: Array[String]): Unit = {
    val set = jedis.smembers("dau:2020-03-30")
    for(e <- set){
      println(e)
    }
  }

}

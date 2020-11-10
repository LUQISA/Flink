package com.atguigu.chapter05

import com.atguigu.chapter05.Flink05_Transform_Map.WaterSensor
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  *
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink20_Sink_Redis {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 2.读取数据
    //    val sensorDS: DataStream[String] = env.readTextFile("input/sensor-data.log")
    val inputDS: DataStream[String] = env.socketTextStream("localhost", 9999)

    val mapDS: DataStream[WaterSensor] = inputDS.map(
      lines => {
        val datas: Array[String] = lines.split(",")
        WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
      }
    )

    // 4.保存到Redis
    // 4.1 创建配置对象
    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setHost("hadoop102")
      .setPort(6379)
      .build()


    mapDS.addSink(
      new RedisSink[WaterSensor](
        config,
        new RedisMapper[WaterSensor] {
          override def getCommandDescription: RedisCommandDescription = {
            // 这里的key是redis最外层的key
            new RedisCommandDescription(RedisCommand.HSET, "sensor0317")
          }

          // 数据类型，Hash类型的key
          override def getKeyFromData(data: WaterSensor): String = {
            data.id
          }

          // 数据类型，Hash类型的value
          override def getValueFromData(data: WaterSensor): String = {
            data.vc.toString
          }
        }
      )
    )

    // 5. 执行
    env.execute()
  }


  // 定义样例类：水位传感器：用于接收空高数据
  // id:传感器编号
  // ts:时间戳
  // vc:空高 (为了方便，在本次课程当成水位高度来使用)
  case class WaterSensor(id: String, ts: Long, vc: Int)

}

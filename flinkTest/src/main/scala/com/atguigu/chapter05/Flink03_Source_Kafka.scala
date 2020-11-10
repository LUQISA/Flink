package com.atguigu.chapter05

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
  * 从Kafka中读取数据
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink03_Source_Kafka {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 2. 从kafka中读取数据
    // 2.1 创建配置对象
    val properties = new Properties()
    // 2.2 添加kafka和zk的配置
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    // 2.3 连接kafka读取数据
    val kafkaDS: DataStream[String] = env.addSource(
      new FlinkKafkaConsumer011 [String](
        "sensor0317",
        new SimpleStringSchema(),
        properties
      )
    )

    kafkaDS.print("kafka source")


    // 3. 执行
    env.execute("kafka source job")
  }


  // 定义样例类：水位传感器：用于接收空高数据
  // id:传感器编号
  // ts:时间戳
  // vc:空高 (为了方便，在本次课程当成水位高度来使用)
  case class WaterSensor(id: String, ts: Long, vc: Int)

}

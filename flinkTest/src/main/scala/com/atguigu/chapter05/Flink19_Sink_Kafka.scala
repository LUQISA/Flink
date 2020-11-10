package com.atguigu.chapter05

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.util.Collector

/**
  *
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink19_Sink_Kafka {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 2.读取数据
//    val sensorDS: DataStream[String] = env.readTextFile("input/sensor-data.log")
    val inputDS: DataStream[String] = env.socketTextStream("localhost",9999)


    // 4.保存到kafka
    inputDS.addSink(
      new FlinkKafkaProducer011[String](
        "hadoop102:9092,hadoop103:9092,hadoop104:9092",
        "sinktest",
        new SimpleStringSchema()
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

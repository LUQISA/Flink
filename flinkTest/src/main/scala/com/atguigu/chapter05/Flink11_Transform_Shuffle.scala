package com.atguigu.chapter05

import org.apache.flink.streaming.api.scala._

/**
  *
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink11_Transform_Shuffle {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    // 2.读取数据
    val sensorDS: DataStream[String] = env.readTextFile("input/sensor-data.log")

    val shuffleDS: DataStream[String] = sensorDS.shuffle

    sensorDS.print("data")
    shuffleDS.print("shuffle")

    // 4. 执行
    env.execute()
  }


  // 定义样例类：水位传感器：用于接收空高数据
  // id:传感器编号
  // ts:时间戳
  // vc:空高 (为了方便，在本次课程当成水位高度来使用)
  case class WaterSensor(id: String, ts: Long, vc: Int)

}

package com.atguigu.chapter05

import org.apache.flink.streaming.api.scala._

/**
  * 从集合中读取数据
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink01_Source_Collection {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 2. 从集合中读取数据
    val listDS: DataStream[WaterSensor] = env.fromCollection(
      //      List(1, 2, 3, 4, 5)
      List(
        WaterSensor("ws_001", 1577844001, 45),
        WaterSensor("ws_002", 1577844015, 43),
        WaterSensor("ws_003", 1577844020, 42)
      )

    )

    listDS.print()

    // 3. 执行
    env.execute()
  }


  // 定义样例类：水位传感器：用于接收空高数据
  // id:传感器编号
  // ts:时间戳
  // vc:空高 (为了方便，在本次课程当成水位高度来使用)
  case class WaterSensor(id: String, ts: Long, vc: Int)

}

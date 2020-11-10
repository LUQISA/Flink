package com.atguigu.chapter05

import org.apache.flink.streaming.api.scala._

/**
  *
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink14_Transform_Connect {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    // 2.读取数据
    val sensorDS: DataStream[String] = env.readTextFile("input/sensor-data.log")

    // 3.转换成样例类
    val mapDS: DataStream[WaterSensor] = sensorDS.map(
      lines => {
        val datas: Array[String] = lines.split(",")
        WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
      }
    )

    // 4. 从集合中再读取一条流
    val numDS: DataStream[Int] = env.fromCollection(List(1, 2, 3, 4, 5, 6))

    // connect连接起来的流，还是各管各的，各自保持自己的类型
    val resultCS: ConnectedStreams[WaterSensor, Int] = mapDS.connect(numDS)

    // coMap表示连接流调用的map，各自都需要一个 function
    resultCS
      .map(
        sensor => sensor.id,
        num => num + 1
      )
      .print()

    // 5. 执行
    env.execute()
  }


  // 定义样例类：水位传感器：用于接收空高数据
  // id:传感器编号
  // ts:时间戳
  // vc:空高 (为了方便，在本次课程当成水位高度来使用)
  case class WaterSensor(id: String, ts: Long, vc: Int)

}

package com.atguigu.chapter05

import org.apache.flink.streaming.api.scala._

/**
  *
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink17_Transform_Reduce {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 2.读取数据
    val sensorDS: DataStream[String] = env.readTextFile("input/sensor-data.log")
    // 3.转成样例类
    val mapDS: DataStream[WaterSensor] = sensorDS.map(
      lines => {
        val datas: Array[String] = lines.split(",")
        WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
      }
    )

    // 4. 按照id进行分组
    val sensorKS: KeyedStream[WaterSensor, String] = mapDS.keyBy(_.id)

    // TODO reduce是keyby之后调用的
    // 输入的类型一样，输出类型和输出类型也要一样
    // 组内的第一条数据，不进入reduce计算
    val reduceDS: DataStream[WaterSensor] = sensorKS.reduce(
      (ws1, ws2) => {
        println(ws1 + "<===>" + ws2)
        WaterSensor(ws1.id, System.currentTimeMillis(), ws1.vc + ws2.vc)
      }
    )
    reduceDS.print("reduce")

    // 5. 执行
    env.execute()
  }


  // 定义样例类：水位传感器：用于接收空高数据
  // id:传感器编号
  // ts:时间戳
  // vc:空高 (为了方便，在本次课程当成水位高度来使用)
  case class WaterSensor(id: String, ts: Long, vc: Int)

}

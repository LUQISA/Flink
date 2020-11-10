package com.atguigu.chapter05

import com.atguigu.chapter05.Flink10_Transform_KeyBy.WaterSensor
import org.apache.flink.streaming.api.scala._

/**
  *
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink16_Transform_RollAgg {
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

    val dataDS: DataStream[(String, Int, Int)] = mapDS.map(sensor => (sensor.id, sensor.vc, 1))


    // 4. 按照id进行分组
    val sensorKS: KeyedStream[(String, Int, Int), String] = dataDS.keyBy(_._1)

    // 5. 聚合操作：keyby之后调用,聚合是组内聚合
    // TODO 滚动聚合算子：来一条、算一次、输出一次
    //    val sumDS: DataStream[(String, Int)] = sensorKS.sum(2)
    val minDS: DataStream[(String, Int, Int)] = sensorKS.min(1)
    val maxDS: DataStream[(String, Int, Int)] = sensorKS.max(1)


//    sumDS.print("sum")
//    minDS.print("min")
    maxDS.print("max")

    // 5. 执行
    env.execute()
  }


  // 定义样例类：水位传感器：用于接收空高数据
  // id:传感器编号
  // ts:时间戳
  // vc:空高 (为了方便，在本次课程当成水位高度来使用)
  case class WaterSensor(id: String, ts: Long, vc: Int)

}

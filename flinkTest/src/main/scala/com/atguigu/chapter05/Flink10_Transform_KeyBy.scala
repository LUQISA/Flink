package com.atguigu.chapter05

import com.atguigu.chapter05.Flink05_Transform_Map.WaterSensor
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

/**
  *
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink10_Transform_KeyBy {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 2.读取数据
    val sensorDS: DataStream[String] = env.readTextFile("input/sensor-data.log")
//    val sensorDS: DataStream[String] = env.readTextFile("F:\\atguigu\\01_course\\code\\flink0317\\input\\sensor-data.log")
//    val sensorDS: DataStream[String] = env.socketTextStream("localhost",9999)

    // 3.转换成样例类 WaterSensor
    val mapDS: DataStream[WaterSensor] = sensorDS.map(
      lines => {
        val datas: Array[String] = lines.split(",")
        WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
      }
    )

    // 4. 使用keyby进行分组
    // TODO 关于返回的key的类型：
    // 1. 如果是位置索引 或 字段名称 ，程序无法推断出key的类型，所以给一个java的Tuple类型
    // 2. 如果是匿名函数 或 函数类 的方式，可以推断出key的类型，比较推荐使用
    // *** 分组的概念：分组只是逻辑上进行分组,打上了记号(标签)，跟并行度没有绝对的关系
    //      同一个分组的数据在一起（不离不弃）
    //      同一个分区里可以有多个不同的组

    //        val sensorKS: KeyedStream[WaterSensor, Tuple] = mapDS.keyBy(0)
    //    val sensorKS: KeyedStream[WaterSensor, Tuple] = mapDS.keyBy("id")
    val sensorKS: KeyedStream[WaterSensor, String] = mapDS.keyBy(_.id)
    //    val sensorKS: KeyedStream[WaterSensor, String] = mapDS.keyBy(
    //      new KeySelector[WaterSensor, String] {
    //        override def getKey(value: WaterSensor): String = {
    //          value.id
    //        }
    //      }
    //    )

    sensorKS.print().setParallelism(5)

    // 4. 执行
    env.execute()
  }


  // 定义样例类：水位传感器：用于接收空高数据
  // id:传感器编号
  // ts:时间戳
  // vc:空高 (为了方便，在本次课程当成水位高度来使用)
  case class WaterSensor(id: String, ts: Long, vc: Int)

}

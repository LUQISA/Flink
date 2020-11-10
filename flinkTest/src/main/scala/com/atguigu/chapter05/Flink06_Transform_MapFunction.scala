package com.atguigu.chapter05

import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction, RuntimeContext}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
  * Transform-mapfunction
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink06_Transform_MapFunction {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 2.读取数据
    val sensorDS: DataStream[String] = env.readTextFile("input/sensor-data.log")

    // 3.使用map操作
    val myMapDS: DataStream[WaterSensor] = sensorDS.map(new MyMapFunction())

    myMapDS.print("my map")


    // 4. 执行
    env.execute()
  }


  // 自定义 map 算子的函数类 MapFunction
  // 1. 实现MapFunction,指定输入、输出的泛型
  // 2. 重写 map 方法
  class MyMapFunction extends MapFunction[String, WaterSensor] {
    override def map(value: String): WaterSensor = {
      val datas: Array[String] = value.split(",")
      WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
    }


  }


  // 定义样例类：水位传感器：用于接收空高数据
  // id:传感器编号
  // ts:时间戳
  // vc:空高 (为了方便，在本次课程当成水位高度来使用)
  case class WaterSensor(id: String, ts: Long, vc: Int)

}

package com.atguigu.chapter05

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
  * Transform-mapfunction
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink07_Transform_RichMapFunction {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 2.读取数据
//    val sensorDS: DataStream[String] = env.readTextFile("input/sensor-data.log")
        val sensorDS: DataStream[String] = env.socketTextStream("localhost",9999)

    // 3.使用map操作
    val myMapDS: DataStream[WaterSensor] = sensorDS.map(new MyMapFunction())

    myMapDS.print("my map")


    // 4. 执行
    env.execute()
  }


  // 富函数：函数类的Rich版本
  // 比普通函数类富有的地方：
  // 1. 有生命周期管理方法 ： open、close
  // 2. 能获取运行时上下文对象： getRuntimeContext, 可以用来获取 state、task信息、并行度信息
  class MyMapFunction extends RichMapFunction[String, WaterSensor] {
    var count: Int = 0

    override def map(value: String): WaterSensor = {
      val datas: Array[String] = value.split(",")
      //      WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
      count += 1
      WaterSensor(getRuntimeContext.getTaskName, datas(1).toLong, datas(2).toInt)
    }

    // 富函数提供了生命周期方法
    override def open(parameters: Configuration): Unit = {
      println("open" + count)
    }

    override def close(): Unit = {
      println("close" + count)
    }

    //    override def getRuntimeContext: RuntimeContext = super.getRuntimeContext

  }


  // 定义样例类：水位传感器：用于接收空高数据
  // id:传感器编号
  // ts:时间戳
  // vc:空高 (为了方便，在本次课程当成水位高度来使用)
  case class WaterSensor(id: String, ts: Long, vc: Int)

}

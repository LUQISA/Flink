package com.atguigu.chapter05

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  *
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink18_Transform_Process {
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

    // TODO process
    val processDS: DataStream[String] = sensorKS.process(new MyKeyedProcessFunction())

    processDS.print("process")

    // 5. 执行
    env.execute()
  }

  // 自定义KeyedProcessFunction,是一个特殊的富函数
  // 1.实现KeyedProcessFunction，指定泛型：K - key的类型， I - 上游数据的类型， O - 输出的数据类型
  // 2.重写 processElement方法，定义 每条数据来的时候 的 处理逻辑

  class MyKeyedProcessFunction extends KeyedProcessFunction[String, WaterSensor, String] {
    /**
      * 处理逻辑：来一条处理一条
      *
      * @param value 一条数据
      * @param ctx   上下文对象
      * @param out   采集器：收集数据，并输出
      */
    override def processElement(value: WaterSensor, ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, out: Collector[String]): Unit = {
      out.collect("我来到process啦，分组的key是="+ctx.getCurrentKey+",数据=" + value)
      // 如果key是tuple，即keyby的时候，使用的是 位置索引 或 字段名称，那么key获取到是一个tuple
//      ctx.getCurrentKey.asInstanceOf[Tuple1].f0 //Tuple1需要手动引入Java的Tuple
    }


  }


  // 定义样例类：水位传感器：用于接收空高数据
  // id:传感器编号
  // ts:时间戳
  // vc:空高 (为了方便，在本次课程当成水位高度来使用)
  case class WaterSensor(id: String, ts: Long, vc: Int)

}

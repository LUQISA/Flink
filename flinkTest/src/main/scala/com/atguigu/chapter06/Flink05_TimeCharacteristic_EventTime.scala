package com.atguigu.chapter06

import com.atguigu.chapter05.Flink17_Transform_Reduce.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * 全窗口函数
  *
  * @version 1.0
  * @author create by cjp on 2020/8/31 10:37
  */
object Flink05_TimeCharacteristic_EventTime {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // TODO 在执行环境里，指定时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2.读取数据,转换成样例类
    val socketDS: DataStream[WaterSensor] = env
      .socketTextStream("localhost", 9999)
      .map(
        line => {
          val datas: Array[String] = line.split(",")
          WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
        })
      // TODO 指定数据里作为事件时间的字段,Flink里的时间戳都是毫秒
      .assignAscendingTimestamps(_.ts * 1000L)

    // 3.转换成二元组、分组、开窗、聚合
    val resultDS: DataStream[(String, Int)] = socketDS
      .map(data => (data.id, 1))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .sum(1)

    // 4. 打印
    resultDS.print("event time")


    // 执行
    env.execute()
  }
}

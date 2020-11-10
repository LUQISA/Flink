package com.atguigu.chapter06

import com.atguigu.chapter05.Flink05_Transform_Map.WaterSensor
import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  * 窗口API
  *
  * @version 1.0
  * @author create by cjp on 2020/8/31 10:37
  */
object Flink03_Window_AggFunction {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) //
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //事件语义

    // 2.读取数据
    val socketDS: DataStream[WaterSensor] = env.socketTextStream("localhost", 9999)
      .map(
        lines => {
          val datas: Array[String] = lines.split(",")
          WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
        }
      )
      .assignAscendingTimestamps(_.ts * 1000L)

    // 3.分组、开窗
    // 正常来说，使用窗口，都是在分组之后
    val dataWS: WindowedStream[(String, Int), String, TimeWindow] = socketDS
      .map(data => (data.id, 1))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10)) // 滚动窗口

    // 4.使用窗口的增量聚合函数进行聚合
    //    val reduceDS: DataStream[(String, Int)] = dataWS
    //      .reduce(
    //        new ReduceFunction[(String, Int)] {
    //          override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
    //            (value1._1, value1._2 + value2._2)
    //          }
    //        }
    //      )

    dataWS
      .aggregate(
        new AggregateFunction[(String, Int), Long, Long] {
          override def createAccumulator(): Long = 0L

          override def add(value: (String, Int), accumulator: Long): Long = {
            println("add....")
            value._2 + accumulator
          }

          override def getResult(accumulator: Long): Long = {
            println("get result")
            accumulator
          }

          override def merge(a: Long, b: Long): Long = a + b
        }
      )
      .print("window aggregate ")


    // 5.打印
    //    reduceDS.print("window reduce")


    // 执行
    env.execute()
  }
}

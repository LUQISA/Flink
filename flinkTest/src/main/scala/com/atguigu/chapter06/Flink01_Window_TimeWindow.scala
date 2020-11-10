package com.atguigu.chapter06

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{ProcessingTimeSessionWindows, SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  * 时间窗口
  *
  * @version 1.0
  * @author create by cjp on 2020/8/31 10:37
  */
object Flink01_Window_TimeWindow {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 2.读取数据
    val socketDS: DataStream[String] = env.socketTextStream("localhost", 9999)

    // 如果 DataStream 直接开窗，带all，表示开了一窗口，并行度可能变成1，所有数据都经过这个窗口
    //    socketDS.windowAll()
    //        socketDS.timeWindowAll()
    //    socketDS.countWindowAll()

    // 3.
    // 正常来说，使用窗口，都是在分组之后
    val dataWS: WindowedStream[(String, Int), String, TimeWindow] = socketDS
      .map(data => (data, 1))
      .keyBy(_._1)
      //      .timeWindow(Time.seconds(3)) // 滚动窗口
      //      .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))

      //      .timeWindow(Time.seconds(3),Time.seconds(2))  // 滑动窗口，多传一个 滑动间隔
      //    .window(SlidingProcessingTimeWindows.of(Time.seconds(3),Time.seconds(2))
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(3)))
    // 4.聚合
    val resultDS: DataStream[(String, Int)] = dataWS.sum(1)

    // 5.打印
    resultDS.print("time window")


    // 执行
    env.execute()
  }
}

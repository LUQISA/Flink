package com.atguigu.chapter06

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}

/**
  * 计数窗口
  *
  * @version 1.0
  * @author create by cjp on 2020/8/31 10:37
  */
object Flink02_Window_CountWindow {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 2.读取数据
    val socketDS: DataStream[String] = env.socketTextStream("localhost", 9999)

    // 3.
    // 正常来说，使用窗口，都是在分组之后
    val dataWS: WindowedStream[(String, Int), String, GlobalWindow] = socketDS
      .map(data => (data, 1))
      .keyBy(_._1)
      //      .countWindow(3) // 滚动窗口，同一组的数据条数达到，就触发计算、关窗

      // 滑动窗口，每经过一个滑动步长，就会触发一次计算
      .countWindow(3, 2)

    dataWS.sum(1).print("count window")


    // 执行
    env.execute()
  }
}

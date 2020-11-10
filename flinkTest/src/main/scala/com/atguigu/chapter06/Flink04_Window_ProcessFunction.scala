package com.atguigu.chapter06

import org.apache.flink.api.common.functions.AggregateFunction
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
object Flink04_Window_ProcessFunction {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 2.读取数据
    val socketDS: DataStream[String] = env.socketTextStream("localhost", 9999)

    // 3.分组、开窗
    // 正常来说，使用窗口，都是在分组之后
    val dataWS: WindowedStream[(String, Int), String, TimeWindow] = socketDS
      .map(data => (data, 1))
      .keyBy(_._1)
      .timeWindow(Time.seconds(3)) // 滚动窗口

    // 4.使用全窗口函数进行处理
    // 全窗口，表示不是来一条处理一条，而是等数据都到齐了，窗口要计算的时候 才 一起对所有数据进行计算
    val processDS: DataStream[String] = dataWS
      .process(
        new ProcessWindowFunction[(String, Int), String, String, TimeWindow] {
          /**
            * 对窗口内，同一分组的所有数据进行处理，即一次进来的是一个分组的数据，不同分组都会执行这个逻辑
            *
            * @param key
            * @param context
            * @param elements
            * @param out
            */
          override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[String]): Unit = {
            out.collect(
              "当前的key是=" + key
                + "，属于窗口[" + context.window.getStart + "," + context.window.getEnd
                + "],一共有数据" + elements.size + "条数据"
            )
          }
        }
      )

    // 5. 打印
    processDS.print("window process")




    // 执行
    env.execute()
  }
}

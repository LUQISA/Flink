package com.atguigu.chapter06

import com.atguigu.chapter05.Flink17_Transform_Reduce.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  *
  *
  * @version 1.0
  * @author create by cjp on 2020/8/31 10:37
  */
object Flink07_WaterMark_Ascending {
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
      // 这种调用方式，要求数据本身没有乱序，是升序的
      // watermark = eventtime - 1ms
      .assignAscendingTimestamps(_.ts * 1000L)

    // 3.转换成二元组、分组、开窗、聚合

    //TODO 窗口如何划分？
    // 1. 窗口的开始时间
    //    timestamp - (timestamp + windowSize) % windowSize
    //    1549044122 - (1549044122 + 5) % 5
    //  => 1549044122 - 2 = 1549044120 => 取窗口长度的整数倍，简单理解为取整
    // 2. 窗口的结束时间 =》 开始时间 + 窗口长度 =》 1549044120 + 5 = 1549044125
    // 3. 窗口是 左闭右开 [1549044120, 1549044125)

    //TODO 窗口如何触发计算的？
    // Watermark达到窗口的结束时间，就会触发计算
    // window.maxTimestamp() <= ctx.getCurrentWatermark() 时，触发计算,即 由watermark触发计算
    // => maxTimestamp = end - 1 ，Gets the largest timestamp that still belongs to this window.所以是左闭右开
    val resultDS: DataStream[String] = socketDS
      .map(data => (data.id, 1))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .process(
        new ProcessWindowFunction[(String, Int), String, String, TimeWindow] {
          override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[String]): Unit = {
            out.collect(
              "当前的key是=" + key
                + "，属于窗口[" + context.window.getStart + "," + context.window.getEnd
                + "]，一共有" + elements.size + "条数据"
                + "，当前的watermark=" + context.currentWatermark
            )
          }
        }
      )

    // 4. 打印
    resultDS.print("event time")


    // 执行
    env.execute()
  }
}

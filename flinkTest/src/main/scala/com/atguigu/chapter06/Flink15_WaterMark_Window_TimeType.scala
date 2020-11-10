package com.atguigu.chapter06

import com.atguigu.chapter05.Flink17_Transform_Reduce.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  *
  *
  * @version 1.0
  * @author create by cjp on 2020/8/31 10:37
  */
object Flink15_WaterMark_Window_TimeType {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2.读取数据,转换成样例类
    val socketDS: DataStream[WaterSensor] = env
      .socketTextStream("localhost", 9999)
      .map(
        line => {
          val datas: Array[String] = line.split(",")
          WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
        })
      .assignAscendingTimestamps(_.ts * 1000L)

    // 3.转换成二元组、分组、开窗、聚合
    val resultDS: DataStream[String] = socketDS
      .map(data => (data.id, 1))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
//      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//      .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(3)))
//      .window(EventTimeSessionWindows.withGap(Time.seconds(3)))
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

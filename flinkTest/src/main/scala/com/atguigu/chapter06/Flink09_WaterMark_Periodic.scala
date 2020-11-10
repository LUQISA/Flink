package com.atguigu.chapter06

import com.atguigu.chapter05.Flink17_Transform_Reduce.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  *
  *
  * @version 1.0
  * @author create by cjp on 2020/8/31 10:37
  */
object Flink09_WaterMark_Periodic {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // TODO 在执行环境里，指定时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 设置周期性生成 watermark的时间间隔
    // 一般还是保持默认，如果要改，还是保持在ms级
    env.getConfig.setAutoWatermarkInterval(3000L)

    // 2.读取数据,转换成样例类
    val socketDS: DataStream[WaterSensor] = env
      .socketTextStream("localhost", 9999)
      .map(
        line => {
          val datas: Array[String] = line.split(",")
          WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
        })

      .assignTimestampsAndWatermarks(
        //getConfig().setAutoWatermarkInterval(200),默认生成周期是200ms
        new AssignerWithPeriodicWatermarks[WaterSensor] {
          // 创建一个变量，用来存储 当前最大的时间戳
          var currentTs: Long = 0L

          override def getCurrentWatermark: Watermark = {
            println("getCurrentWatermark...")
            // 根据当前最大的时间戳，创建watermark
            new Watermark(currentTs)
          }

          override def extractTimestamp(element: WaterSensor, previousElementTimestamp: Long): Long = {
            // 更新存储的当前最大时间戳
            currentTs = currentTs.max(element.ts * 1000L)
            // 返回数据中的时间字段，作为事件时间
            element.ts * 1000L
          }
        }
      )


    // 3.转换成二元组、分组、开窗、聚合
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

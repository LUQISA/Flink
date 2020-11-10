package com.atguigu.chapter06

import com.atguigu.chapter05.Flink17_Transform_Reduce.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * 对迟到数据的处理 - 侧输出流
  *
  * @version 1.0
  * @author create by cjp on 2020/8/31 10:37
  */
object Flink13_WaterMark_SideOutput {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2.读取数据,转换成样例类
    val socketDS: DataStream[WaterSensor] = env
      .socketTextStream("localhost",9999)
      .map(
        line => {
          val datas: Array[String] = line.split(",")
          WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
        })
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[WaterSensor](Time.seconds(3)) {
          override def extractTimestamp(element: WaterSensor): Long = element.ts * 1000L
        }
      )

    //TODO 窗口关闭后 的 迟到数据的处理 —— 侧输出流

    // 3.转换成二元组、分组、开窗、聚合
    // 创建一个侧输出流的标签
    val outputTag = new OutputTag[(String,Int)]("late data")
    val resultDS: DataStream[String] = socketDS
      .map(data => (data.id, 1))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .allowedLateness(Time.seconds(2)) // 设置窗口延迟，等待迟到数据
      .sideOutputLateData(outputTag)  // 窗口关闭后 的 迟到数据，放入到侧输出流中
      .process(
        new ProcessWindowFunction[(String, Int), String, String, TimeWindow] {
          override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[String]): Unit = {
            out.collect(
              "当前的key是=" + key
                + "，属于窗口[" + context.window.getStart + "," + context.window.getEnd
                + "]，数据=" + elements.toString()
                + "，当前的watermark=" + context.currentWatermark
            )
          }
        }
      )

    // 从主流里 获取 侧输出流的数据
    resultDS.getSideOutput(outputTag).print("late data")

    // 4. 打印
    resultDS.print("result data")


    // 执行
    env.execute()
  }
}

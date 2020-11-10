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
  *
  *
  * @version 1.0
  * @author create by cjp on 2020/8/31 10:37
  */
object Flink08_WaterMark_OutOfOrderness {
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
      // 乱序的场景，flink提供了BoundedOutOfOrdernessTimestampExtractor类
      //    需要传两个参数
      //      第一个参数：从数据中抽取事件时间，注意时间单位
      //      第二个参数：在括号里，传入等待的时间。为了解决乱序，解决方案就是 --- 等一会
      // watermark = eventtime - awaittime
      // 由 watermark触发窗口的计算
      .assignTimestampsAndWatermarks(
      // 等待时间，优先考虑最大乱序程度
      // 怎么知道乱序程度 => 经验 、 自己抽样去看 => 一般给多少，建议秒级，最多不要超过分钟级
      // 如果乱序程度特别大，也不一定要设置成最大乱序
        new BoundedOutOfOrdernessTimestampExtractor[WaterSensor](Time.seconds(3)) {
          override def extractTimestamp(element: WaterSensor): Long = {
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

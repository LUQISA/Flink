package com.atguigu.chapter06

import com.atguigu.chapter05.Flink17_Transform_Reduce.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
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
object Flink11_WaterMark_File_Issue  {
  def main(args: Array[String]): Unit = {

    // TODO 注意：当读取的数据源为 文件（有界流） 时， 为了保证所有的数据都能被触发计算，在最后，会给一个Long的最大值的watermark
    // watermark = Long的最大值， 最后一个数据所在的窗口一定会被触发

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // TODO 在执行环境里，指定时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2.读取数据,转换成样例类
    val socketDS: DataStream[WaterSensor] = env
      .readTextFile("input/sensor-data.log")
      .map(
        line => {
          val datas: Array[String] = line.split(",")
          WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
        })

      .assignTimestampsAndWatermarks(
        new AssignerWithPunctuatedWatermarks[WaterSensor]{

          override def checkAndGetNextWatermark(lastElement: WaterSensor, extractedTimestamp: Long): Watermark = {
            println("checkAndGetNextWatermark...")
            new Watermark(extractedTimestamp)
          }

          override def extractTimestamp(element: WaterSensor, previousElementTimestamp: Long): Long = {
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

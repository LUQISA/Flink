package com.atguigu.chapter06

import java.sql.Timestamp

import com.atguigu.chapter05.Flink17_Transform_Reduce.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.util.Collector

/**
  *
  *
  * @version 1.0
  * @author create by cjp on 2020/8/31 10:37
  */
object Flink19_ProcessFunction_SideOutput {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2.读取数据,转换成样例类
    val socketDS: DataStream[WaterSensor] = env
      .socketTextStream("localhost", 9999)
      .map(
        line => {
          val datas: Array[String] = line.split(",")
          WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
        })
      .assignTimestampsAndWatermarks(
        new AssignerWithPunctuatedWatermarks[WaterSensor]{

          override def checkAndGetNextWatermark(lastElement: WaterSensor, extractedTimestamp: Long): Watermark = {
            new Watermark(extractedTimestamp)
          }

          override def extractTimestamp(element: WaterSensor, previousElementTimestamp: Long): Long = {
            element.ts * 1000L
          }
        }
      )

    // 3.转换成二元组、分组、开窗、聚合
    val processDS: DataStream[WaterSensor] = socketDS
      .keyBy(_.id)
      .process(
        // 实现 监控水位连续5s内上涨，则告警
        new KeyedProcessFunction[String, WaterSensor, WaterSensor] {

          private var currentHeight: Int = 0
          private var timerTs: Long = 0L

          override def processElement(value: WaterSensor, ctx: KeyedProcessFunction[String, WaterSensor, WaterSensor]#Context, out: Collector[WaterSensor]): Unit = {

            if (value.vc > 5) {
              // 如果水位大于5，那么就将告警信息放入测输出流
              val outputTag = new OutputTag[String]("high water level")
              ctx.output(outputTag, "水位超过阈值：" + value.toString)
            } else {
              // 如果水位不大于5，那么就正常往下游传递数据
              out.collect(value)
            }

          }

        }
      )

    processDS.print("result")
    val outputTag = new OutputTag[String]("high water level")
    processDS.getSideOutput(outputTag).print("alarm")


    // 执行
    env.execute()
  }
}

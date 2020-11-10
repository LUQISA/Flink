package com.atguigu.chapter06

import java.sql.Timestamp

import com.atguigu.chapter05.Flink17_Transform_Reduce.WaterSensor
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
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
object Flink22_State_Keyed {
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
        new AssignerWithPunctuatedWatermarks[WaterSensor] {

          override def checkAndGetNextWatermark(lastElement: WaterSensor, extractedTimestamp: Long): Watermark = {
            new Watermark(extractedTimestamp)
          }

          override def extractTimestamp(element: WaterSensor, previousElementTimestamp: Long): Long = {
            element.ts * 1000L
          }
        }
      )

    // 3.转换成二元组、分组、开窗、聚合
    socketDS
      .keyBy(_.id)
      .process(
        // 实现 监控水位连续5s内上涨，则告警
        new KeyedProcessFunction[String, WaterSensor, String] {

          //          var count: Long = 0L
          private var countState: ValueState[Long] = _

          override def open(parameters: Configuration): Unit = {
            countState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))
          }

          override def processElement(value: WaterSensor, ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, out: Collector[String]): Unit = {
            //            count += 1
            var currentCount: Long = countState.value()
            currentCount += 1L
            countState.update(currentCount)
            //            println("当前key=" + ctx.getCurrentKey + value.toString+",count值 = "+count)
            println("当前key=" + ctx.getCurrentKey + value.toString + ",count值 = " + countState.value())
          }
        }
      )
      .print("timer")


    // 4. 打印


    // 执行
    env.execute()
  }
}

package com.atguigu.chapter06

import java.sql.Timestamp

import com.atguigu.chapter05.Flink17_Transform_Reduce.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
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
object Flink16_ProcessFunction_Timer {
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
    socketDS
      .keyBy(_.id)
      .process(
        new KeyedProcessFunction[String, WaterSensor, String] {
          /**
            * 处理数据的方法：来一条处理一条
            *
            * @param value 当前数据
            * @param ctx   上下文对象
            * @param out   采集器：往下游发送数据
            */
          override def processElement(value: WaterSensor, ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, out: Collector[String]): Unit = {
            //  获取当前的 key
            //            ctx.getCurrentKey
            //  获取时间戳
            //            ctx.timestamp()
            //  将数据放入侧输出流
            //            ctx.output()
            //  定时器： 获取时间信息（两种语义）、定时器的注册和删除
            //            ctx.timerService().registerEventTimeTimer() // 注册基于事件时间的定时器
            //            ctx.timerService().registerProcessingTimeTimer()  //注册基于处理时间的定时器
            //            ctx.timerService().deleteEventTimeTimer() // 删除基于事件时间的定时器
            //            ctx.timerService().deleteProcessingTimeTimer() // 删除基于处理时间的定时器
            //            ctx.timerService().currentProcessingTime()  // 获取当前的处理时间
            //            ctx.timerService().currentWatermark() // 获取当前的 Watermark

            // 注册一个定时器
            //            ctx.timerService().registerEventTimeTimer(value.ts * 1000L + 5000L)
            println("注册定时器，注册时间为 = " + new Timestamp(ctx.timerService().currentProcessingTime()))
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000L)
          }

          /**
            * 当时间到达 定时器 注册的时间，要做什么操作
            *
            * @param timestamp 定时器触发的时间
            * @param ctx       上下文
            * @param out       采集器
            */
          override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, WaterSensor, String]#OnTimerContext, out: Collector[String]): Unit = {
            println("定时器触发，触发时间 = " + new Timestamp(timestamp))
          }
        }
      )


    // 4. 打印


    // 执行
    env.execute()
  }
}

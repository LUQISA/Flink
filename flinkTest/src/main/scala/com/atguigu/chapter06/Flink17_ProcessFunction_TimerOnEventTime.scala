package com.atguigu.chapter06

import java.sql.Timestamp

import com.atguigu.chapter05.Flink17_Transform_Reduce.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  *
  *
  * @version 1.0
  * @author create by cjp on 2020/8/31 10:37
  */
object Flink17_ProcessFunction_TimerOnEventTime {
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
      .assignAscendingTimestamps(_.ts * 1000L)

    // 3.转换成二元组、分组、开窗、聚合
    socketDS
      .keyBy(_.id)
      .process(
        new KeyedProcessFunction[String, WaterSensor, String] {

          private var timerTs: Long = 0L

          /**
            * 处理数据的方法：来一条处理一条
            *
            * @param value 当前数据
            * @param ctx   上下文对象
            * @param out   采集器：往下游发送数据
            */
          override def processElement(value: WaterSensor, ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, out: Collector[String]): Unit = {

            // 注册一个定时器
            if (timerTs == 0) {
              timerTs = value.ts * 1000L + 5000L
              ctx.timerService().registerEventTimeTimer(timerTs)
              println("注册定时器，注册时间为 = " + new Timestamp(timerTs))
            }
//            ctx.timerService().registerEventTimeTimer(1549044130000L)
//            println("注册定时器，注册时间为 = " + new Timestamp(1549044130000L))
          }

          /**
            * 当时间到达 定时器 注册的时间，要做什么操作
            * 只要是EventTime语义，由 watermark触发计算
            *
            * @param timestamp 定时器触发的时间
            * @param ctx       上下文
            * @param out       采集器
            */
          override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, WaterSensor, String]#OnTimerContext, out: Collector[String]): Unit = {
            println("定时器触发，触发时间 = " + new Timestamp(timestamp) + ",watermark = " + ctx.timerService().currentWatermark())
          }
        }
      )
        .print("timer")


    // 4. 打印


    // 执行
    env.execute()
  }
}

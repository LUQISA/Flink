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
object Flink21_State_StatePractice {
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

          private var currentHeight: ValueState[Int] = _

          private var timerTs: ValueState[Long] = _


          override def open(parameters: Configuration): Unit = {
            currentHeight = getRuntimeContext.getState(new ValueStateDescriptor[Int]("height", classOf[Int]))
            timerTs = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer", classOf[Long]))
          }

          override def processElement(value: WaterSensor, ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, out: Collector[String]): Unit = {

            println("当前key=" + ctx.getCurrentKey + ",state保存的水位值=" + currentHeight.value())
            // 1.判断水位是否上涨
            if (value.vc >= currentHeight.value()) {
              // 1.1 水位上涨
              // 第一条数据来的时候，先注册一个 5s的定时器
              // 如果值状态是一个数值类型，那么默认值就是 0
              if (timerTs.value() == 0) {
                // 保存定时器的时间
                timerTs.update(value.ts * 1000L + 5000L)
                ctx.timerService().registerEventTimeTimer(timerTs.value())
              }
              // 更新保存的水位值
              currentHeight.update(value.vc)
            } else {
              // 1.2 水位下降:
              // 1.2.1、删除原来的定时器；
              ctx.timerService().deleteEventTimeTimer(timerTs.value())
              // 2、重新注册定时器（把定时器清零）;
              timerTs.clear()
              // 3、更新当前水位值
              currentHeight.update(value.vc)
            }

          }

          /**
            * 定时器触发：表示连续 5s水位上升，要告警
            *
            * @param timestamp
            * @param ctx
            * @param out
            */
          override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, WaterSensor, String]#OnTimerContext, out: Collector[String]): Unit = {
            out.collect("在" + new Timestamp(timestamp) + "监测到连续 5s水位上涨")
            // 定时器触发后，清空保存的时间，避免影响后续的告警
            timerTs.clear()
          }
        }
      )
      .print("timer")


    // 4. 打印


    // 执行
    env.execute()
  }
}

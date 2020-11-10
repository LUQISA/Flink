package com.atguigu.chapter05

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random

/**
  * 从自定义数据源中读取数据
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink04_Source_MySource {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //
    val myDS: DataStream[WaterSensor] = env.addSource(new MySourceFunction())

    myDS.print("my source")

    // 3. 执行
    env.execute()
  }

  // 自定义Source
  // 1. 实现SourceFunction，指定泛型
  // 2. 重写 run 和 cancel 两个方法
  class MySourceFunction extends SourceFunction[WaterSensor] {

    var flag: Boolean = true

    override def run(ctx: SourceFunction.SourceContext[WaterSensor]): Unit = {
      while (flag) {
        ctx.collect(
          WaterSensor("sensor_" + (Random.nextInt(3) + 1), System.currentTimeMillis(), 40 + Random.nextInt(10))
        )
        Thread.sleep(1000L)
      }
    }

    override def cancel(): Unit = {
      flag = false
    }
  }


  // 定义样例类：水位传感器：用于接收空高数据
  // id:传感器编号
  // ts:时间戳
  // vc:空高 (为了方便，在本次课程当成水位高度来使用)
  case class WaterSensor(id: String, ts: Long, vc: Int)

}

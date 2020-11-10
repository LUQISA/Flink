package com.atguigu.chapter06

import com.atguigu.chapter05.Flink17_Transform_Reduce.WaterSensor
import org.apache.flink.api.common.state._
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
object Flink20_State_KeyedDemo {
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
    val processDS: DataStream[String] = socketDS
      .keyBy(_.id)
//      .flatMapWithState()
//      .filterWithState()
      .process(
        new KeyedProcessFunction[String, WaterSensor, String] {

          // TODO 获取值类型的状态
          // 1、两种方式获取状态：因为状态是由运行时上下文获取的，运行时上下文只有运行的时候才存在
          //    第一种：通过 lazy 来实现
          //    第二种：在open方法里获取状态
          //          private lazy val state: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("value state",classOf[Long]))
          var state: ValueState[Long] = _
          var listState: ListState[Long] = _
          var mapState: MapState[String,Long] = _

          override def open(parameters: Configuration): Unit = {
            state = getRuntimeContext.getState(new ValueStateDescriptor[Long]("value state", classOf[Long]))
            listState = getRuntimeContext.getListState(new ListStateDescriptor[Long]("list state",classOf[Long]))
            mapState = getRuntimeContext.getMapState(new MapStateDescriptor[String,Long]("map state",classOf[String],classOf[Long]))
          }

          override def processElement(value: WaterSensor, ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, out: Collector[String]): Unit = {
            // 值类型的操作
//            state.update(1L)  //值类型的更新操作
//            state.value() // 值类型的状态获取值
//            state.clear() // 清空状态
            // 列表类型
//            listState.add() // 添加单个值
//            listState.addAll()  // 添加一个List
//            listState.update()  // 更新整个List
//            listState.clear() // 清空状态

            // Map类型  - 操作基本同 HashMap
//            mapState.put()  // 添加或更新数据
//            mapState.putAll() // 整个Map
//            mapState.remove() // 删除某个值
//            mapState.contains() // 是否包含某个key
//            mapState.get()  // 获取指定key的值
          }
        }
      )

    processDS.print()



    // 执行
    env.execute()
  }
}

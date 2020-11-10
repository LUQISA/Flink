package com.atguigu.chapter05

import org.apache.flink.streaming.api.scala._

/**
  *
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink15_Transform_Union {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 2. 从集合中读取流
    val num1DS: DataStream[Int] = env.fromCollection(List(1, 2, 3, 4))
    val num2DS: DataStream[Int] = env.fromCollection(List(7, 8, 9, 10))
    val num3DS: DataStream[Int] = env.fromCollection(List(17, 18, 19, 110))


    // TODO union 真正将多条流合并成一条流
    // 合并的流，类型必须一致
    // 可以合并多条流，只要类型一致
    num1DS.union(num2DS).union(num3DS)
      .print()

    // 5. 执行
    env.execute()
  }


  // 定义样例类：水位传感器：用于接收空高数据
  // id:传感器编号
  // ts:时间戳
  // vc:空高 (为了方便，在本次课程当成水位高度来使用)
  case class WaterSensor(id: String, ts: Long, vc: Int)

}

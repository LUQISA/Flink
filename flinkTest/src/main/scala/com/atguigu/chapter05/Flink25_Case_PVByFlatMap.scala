package com.atguigu.chapter05

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  *
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink25_Case_PVByFlatMap {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 2.读取数据
    val logDS: DataStream[String] = env.readTextFile("input/UserBehavior.csv")

    logDS
      .flatMap(
        line => {
          val datas: mutable.ArrayOps[String] = line.split(",")
          if (datas(3) == "pv") {
            List(("pv", 1L))
          } else {
            Nil
          }
        }
      )
      .keyBy(_._1)
      .sum(1)
      .print("pv by flatmap")


    // 5. 执行
    env.execute()
  }


  /**
    * 用户行为日志样例类
    *
    * @param userId     用户ID
    * @param itemId     商品ID
    * @param categoryId 商品类目ID
    * @param behavior   用户行为类型
    * @param timestamp  时间戳（秒）
    */
  case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

}

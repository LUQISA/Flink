package com.atguigu.chapter05

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  *
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink24_Case_PVByProcess {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 2.读取数据
    val logDS: DataStream[String] = env.readTextFile("input/UserBehavior.csv")
    // 转换成样例类
    val userBehaviorDS: DataStream[UserBehavior] = logDS.map(
      line => {
        val datas: Array[String] = line.split(",")
        UserBehavior(
          datas(0).toLong,
          datas(1).toLong,
          datas(2).toInt,
          datas(3),
          datas(4).toLong
        )
      }
    )

    // 3.处理数据:不转换成二元组，使用process实现计数
    // 3.1 能过滤，先过滤 => 过滤出需要的数据，减少数据量，优化效率
    val filterDS: DataStream[UserBehavior] = userBehaviorDS.filter(_.behavior == "pv")
    // 3.2 按照 pv 行为进行分组（思路：对要 统计的维度（要统计的维度 就是 点击行为）进行分组）
    filterDS
      .keyBy(_.behavior)
      .process(
        new KeyedProcessFunction[String, UserBehavior, Long] {
          // 定义一个变量，用来计数
          var pvCount: Long = 0L

          override def processElement(value: UserBehavior, ctx: KeyedProcessFunction[String, UserBehavior, Long]#Context, out: Collector[Long]): Unit = {
            // 来一条，计数值 + 1，就能实现统计
            pvCount += 1L
            // 使用采集器，发送统计结果数据
            out.collect(pvCount)
          }
        }
      )
      .print("pv by process")

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

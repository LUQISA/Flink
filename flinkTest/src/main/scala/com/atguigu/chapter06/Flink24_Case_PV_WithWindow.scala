package com.atguigu.chapter06

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  *实时统计每小时的pv
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink24_Case_PV_WithWindow {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

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
      .assignAscendingTimestamps(_.timestamp* 1000L)


    // 3.处理数据：参照wordcount实现的思路
    // 3.1 能过滤，先过滤 => 过滤出需要的数据，减少数据量，优化效率
    val filterDS: DataStream[UserBehavior] = userBehaviorDS.filter(_.behavior == "pv")
    // 3.2 转换成（ pv，1）
    val pvAndOneDS: DataStream[(String, Int)] = filterDS.map(pvData => ("pv", 1))
    // 3.3 按照 pv 进行分组
    val pvAndOneKS: KeyedStream[(String, Int), String] = pvAndOneDS.keyBy(_._1)
    //3.4那每小时开窗
    val pvAndoneWS: WindowedStream[(String, Int), String, TimeWindow] = pvAndOneKS.timeWindow(Time.hours(1))
    // 3.4 按照分组进行统计求和
    val pvDS: DataStream[(String, Int)] = pvAndoneWS.sum(1)

    // 4.保存结果（打印）
    pvDS.print("pv")


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

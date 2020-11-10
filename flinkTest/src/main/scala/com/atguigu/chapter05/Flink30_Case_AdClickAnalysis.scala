package com.atguigu.chapter05

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

/**
  * 统计 各个省份 的 每个广告 的点击量
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink30_Case_AdClickAnalysis {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 2.读取数据
    val adClickDS: DataStream[AdClickLog] = env
      .readTextFile("input/AdClickLog.csv")
      .map(
        line => {
          val datas: Array[String] = line.split(",")
          AdClickLog(
            datas(0).toLong,
            datas(1).toLong,
            datas(2),
            datas(3),
            datas(4).toLong
          )
        }
      )

    // 3.处理数据
    // 3.1 按照统计的维度进行分组（维度：省份、广告），多个维度可以拼接在一起
    val provinceAndAdAndOneKS: KeyedStream[(String, Long), String] = adClickDS
      .map(adClick => (adClick.province + "_" + adClick.adId, 1L))
      .keyBy(_._1)
    // 3.2 按照分组进行求和统计
    provinceAndAdAndOneKS
      .sum(1)
      .print("adclick analysis by province and ad")


    // 5. 执行
    env.execute()
  }


  /**
    * 广告点击样例类
    *
    * @param userId    用户ID
    * @param adId      广告ID
    * @param province  省份
    * @param city      城市
    * @param timestamp 时间戳
    */
  case class AdClickLog(
                         userId: Long,
                         adId: Long,
                         province: String,
                         city: String,
                         timestamp: Long)


}

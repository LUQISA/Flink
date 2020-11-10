package com.atguigu.chapter05

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.util.Random

/**
  * 分渠道 的 APP下载量 统计
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink27_Case_APP_DownloadAnalysis {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 2.读取数据
    val inputDS: DataStream[MarketingUserBehavior] = env.addSource(new MySourceFunction())

    // 3.处理数据
    // 3.1 过滤出下载行为
    val filterDS: DataStream[MarketingUserBehavior] = inputDS.filter(_.behavior == "DOWNLOAD")
    filterDS.print("filter")
    // 3.2 按照统计的维度进行分组
    filterDS
      .map(data => (data.channel, 1))
      .keyBy(_._1)
      .sum(1)
      .print()

    // 5. 执行
    env.execute()
  }


  class MySourceFunction extends SourceFunction[MarketingUserBehavior] {

    var flag: Boolean = true
    //
    val userBehaviorList = List("DOWNLOAD", "INSTALL", "UPDATE", "UNINSTALL")
    val channelList = List("HUAWEI", "XIAOMI", "OPPO", "VIVO")

    override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
      while (flag) {
        ctx.collect(
          MarketingUserBehavior(
            Random.nextInt(100).toLong,
            userBehaviorList(Random.nextInt(userBehaviorList.size)),
            channelList(Random.nextInt(channelList.size)),
            System.currentTimeMillis()
          )
        )
        Thread.sleep(1000L)
      }
    }

    override def cancel(): Unit = {
      flag = false
    }
  }


  /**
    *
    * @param userId    用户ID
    * @param behavior  用户行为：下载、安装、更新、卸载
    * @param channel   渠道（平台）：华为、小米、OPPO、VIVO
    * @param timestamp 时间戳
    */
  case class MarketingUserBehavior(
                                    userId: Long,
                                    behavior: String,
                                    channel: String,
                                    timestamp: Long)


}

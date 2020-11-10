package com.atguigu.chapter06

import com.atguigu.chapter06.Flink26_Case_HotltemAnalysis.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  *
  *实时统计每小时内访问网站的总人数
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink25_Case_UV_WithWindow {
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
      .assignAscendingTimestamps(_.timestamp * 1000L)
    // 3.处理数据:

    // 3.处理数据:
    // 3.1 能过滤，先过滤 => 过滤出需要的数据，减少数据量，优化效率
    val filterDS: DataStream[UserBehavior] = userBehaviorDS.filter(_.behavior == "pv")
    // 3.2 转换成（ uv，用户ID）
    // 第一个给 uv 是为了做 keyby分组，第二个给 用户ID，是为了 存到 Set中进行去重，其他数据（商品、时间）用不到
    val userDS: DataStream[(String, Long)] = filterDS.map(data => ("uv", data.userId))
    // 3.3 按照 uv 进行分组
    val userKS: KeyedStream[(String, Long), String] = userDS.keyBy(_._1)
    // 3.4 按照分组统计 UV值
    val processDS: DataStream[Long] = userKS
      .timeWindow(Time.hours(1))
      //.process(new MyKeyedProcessFunction())
      .process(
      new ProcessWindowFunction[(String,Long),Long,String,TimeWindow] {
        //定义一个set来 存储用户id
        val uvCount: mutable.Set[Long] = mutable.Set[Long]()

        override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[Long]): Unit ={
          //1.便利数据，将userid存入set中
          for (element <- elements) {
            uvCount.add(element._2)
          }

          //2/统计set的长度，并通过采集器往下游发送
          out.collect(uvCount.size)
          //3.清空本次窗口保存的Userid,避免影响下个窗口的数据
          uvCount.clear()



        }
      }
    )
    // 4.保存结果（打印）
    processDS.print("uv")






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

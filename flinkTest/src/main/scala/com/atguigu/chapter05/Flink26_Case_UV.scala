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
object Flink26_Case_UV {
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

    // 3.处理数据:
    // 3.1 能过滤，先过滤 => 过滤出需要的数据，减少数据量，优化效率
    val filterDS: DataStream[UserBehavior] = userBehaviorDS.filter(_.behavior == "pv")
    // 3.2 转换成（ uv，用户ID）
    // 第一个给 uv 是为了做 keyby分组，第二个给 用户ID，是为了 存到 Set中进行去重，其他数据（商品、时间）用不到
    val userDS: DataStream[(String, Long)] = filterDS.map(data => ("uv", data.userId))
    // 3.3 按照 uv 进行分组
    val userKS: KeyedStream[(String, Long), String] = userDS.keyBy(_._1)
    // 3.4 按照分组统计 UV值
    val processDS: DataStream[Long] = userKS.process(new MyKeyedProcessFunction())

    // 4.保存结果（打印）
    processDS.print("uv")


    // 5. 执行
    env.execute()
  }

  class MyKeyedProcessFunction extends KeyedProcessFunction[String, (String, Long), Long] {
    // 定义一个Set用来 存储 用户ID
    val uvCount: mutable.Set[Long] = mutable.Set[Long]()

    override def processElement(value: (String, Long), ctx: KeyedProcessFunction[String, (String, Long), Long]#Context, out: Collector[Long]): Unit = {
      // 1.将用户ID存入Set中
      uvCount.add(value._2)
      // 2.统计Set的长度（元素个数，即UV值）
      out.collect(uvCount.size)
    }
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

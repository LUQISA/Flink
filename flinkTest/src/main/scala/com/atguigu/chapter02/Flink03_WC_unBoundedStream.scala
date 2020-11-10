package com.atguigu.chapter02

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

/**
  * WordCount 流处理实现（无界:kafka、socket）
  *
  * @version 1.0
  * @author create by cjp on 2020/8/26 9:37
  */
object Flink03_WC_unBoundedStream {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 2.读取数据
    val socketDS: DataStream[String] = env.socketTextStream("hadoop102", 9999)

    // 3.处理数据
    // 3.1 扁平化操作，切分成单词数组
    val wordDS: DataStream[String] = socketDS.flatMap(_.split(" "))
    // 3.2 转换成（word,1）二元组
    val wordAndOneDS: DataStream[(String, Int)] = wordDS.map((_, 1))
    // 3.3 按照 word 进行分组
    val wordAndOneKS: KeyedStream[(String, Int), Tuple] = wordAndOneDS.keyBy(0)
    // 3.4 按照分组进行求和
    val sumDS: DataStream[(String, Int)] = wordAndOneKS.sum(1)
    // 3.5 打印
    sumDS.print()

    // 4. 启动
    env.execute()

  }
}

package com.atguigu.chapter02

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

/**
  * 并行度测试
  *
  * @version 1.0
  * @author create by cjp on 2020/8/26 9:37
  */
object Flink04_WC_Parallelism {
  def main(args: Array[String]): Unit = {


    //TODO 并行度优先级
    // 算子的并行度(代码中) > 全局并行度（代码中） > 提交参数 > 配置文件
    // 同一个算子的并行任务，必须同时执行，必须等到有足够的slot
    // 不同算子的任务，可以先后共用一个slot

    // 在开发环境中，如果不指定并行度，默认就是电脑的CPU线程数

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 全局设置并行度
//    env.setParallelism(2)

    // 2.读取数据
    val socketDS: DataStream[String] = env.socketTextStream("hadoop102", 9999)

    // 3.处理数据
    // 3.1 扁平化操作，切分成单词数组
    val wordDS: DataStream[String] = socketDS.flatMap(_.split(" ")).setParallelism(2)
    // 3.2 转换成（word,1）二元组
    val wordAndOneDS: DataStream[(String, Int)] = wordDS.map((_, 1)).setParallelism(3)
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

package com.atguigu.chapter02

import org.apache.flink.streaming.api.scala._

/**
  * WordCount 流处理实现（有界:文件）
  *
  * @version 1.0
  * @author create by cjp on 2020/8/26 9:37
  */
object Flink02_WC_BoundedStream {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 2.读取数据
    val lineDS: DataStream[String] = env.readTextFile("input/word.txt")
//    val lineDS: DataStream[String] = env.readTextFile("/opt/module/data/word.txt")

    // 3. 处理数据
    // 3.1 扁平化操作，切分单词
    val wordDS: DataStream[String] = lineDS.flatMap(_.split(" "))
    // 3.2 转换成（word，1）格式
    val wordAndOneDS: DataStream[(String, Long)] = wordDS.map((_,1L))
    // 3.3 按照 word 分组
    val wordAndOneKS: KeyedStream[(String, Long), String] = wordAndOneDS.keyBy(_._1)
    // 3.4 根据分组结果进行求和
    val sumDS: DataStream[(String, Long)] = wordAndOneKS.sum(1)
    // 3.5 打印
    sumDS.print()

    // 4. 启动
    env.execute()

  }
}

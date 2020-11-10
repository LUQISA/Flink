package com.atguigu.chapter02

import org.apache.flink.api.scala._

/**
  * WordCount 批处理实现
  *
  * @version 1.0
  * @author create by cjp on 2020/8/26 9:37
  */
object Flink01_WC_Batch {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // 2. 读取数据源
    val lineDS: DataSet[String] = env.readTextFile("input/word.txt")

    // 3. 处理数据
    // 3.1 切分
    val wordDS: DataSet[String] = lineDS.flatMap(_.split(" "))

    // 3.2. 转换成（word，1）二元组
    val wordAndOneDS: DataSet[(String, Long)] = wordDS.map((_, 1L))

    // 3.3 按照word单词分组
    //java.lang.UnsupportedOperationException: Aggregate does not support grouping with KeySelector functions, yet.
    val wordAndOneGS: GroupedDataSet[(String, Long)] = wordAndOneDS.groupBy(0)

    // 3.4 求和
    val sumDS: AggregateDataSet[(String, Long)] = wordAndOneGS.sum(1)

    // 3.5 打印
    sumDS.print()
//    Thread.sleep(5000L)

    // 4. 启动: 批处理不需要执行启动操作
//    env.execute()

  }
}

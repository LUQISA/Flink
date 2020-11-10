package com.atguigu.chapter08

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment

object Flink02_SQL_TableAPI {

  def main(args: Array[String]): Unit = {

    val settings = EnvironmentSettings
        .newInstance()
        .useOldPlanner()
        .inStreamingMode()
        .build()
    val env =
      StreamExecutionEnvironment
        .getExecutionEnvironment
    val tableEnv =
      StreamTableEnvironment
        .create(env, settings)

    //2.把datastream转换成table对象
    tableEnv.fromDataStream(sensorDS,'id, 'ts, 'vc)


  }




}

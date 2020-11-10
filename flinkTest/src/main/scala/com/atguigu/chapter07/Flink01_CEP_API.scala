package com.atguigu.chapter07


//
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//
//object Flink01_CEP_API {
//  val env =
//    StreamExecutionEnvironment.getExecutionEnvironment
//  env.setParallelism(1)
//
//  val dataDS = env.readTextFile("input/sensor-data.log")
//
//  val sensorDS = dataDS.map(
//    data => {
//      val datas = data.split(",")
//      WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
//    }
//  )
//
//
//
//
//
//}

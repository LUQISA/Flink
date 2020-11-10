package com.atguigu.chapter05

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
  *
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink21_Sink_ES {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 2.读取数据
        val sensorDS: DataStream[String] = env.readTextFile("input/sensor-data.log")
//    val inputDS: DataStream[String] = env.socketTextStream("localhost", 9999)

    val mapDS: DataStream[WaterSensor] = sensorDS.map(
      lines => {
        val datas: Array[String] = lines.split(",")
        WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
      }
    )

    // 4.保存到 ES
    // 4.1
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("hadoop102", 9200))
    httpHosts.add(new HttpHost("hadoop103", 9200))
    httpHosts.add(new HttpHost("hadoop104", 9200))

    val esSink: ElasticsearchSink[WaterSensor] = new ElasticsearchSink.Builder[WaterSensor](
      httpHosts,
      new ElasticsearchSinkFunction[WaterSensor] {
        override def process(element: WaterSensor, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
          println("start...")
          val dataMap = new util.HashMap[String, String]()
          dataMap.put("data", element.toString)
          val request: IndexRequest = Requests.indexRequest("sensor0317").`type`("reading").source(dataMap)
          indexer.add(request)
          println("end...")
        }
      }
    )
      .build()

    mapDS.addSink(esSink)

    // 5. 执行
    env.execute()
  }


  // 定义样例类：水位传感器：用于接收空高数据
  // id:传感器编号
  // ts:时间戳
  // vc:空高 (为了方便，在本次课程当成水位高度来使用)
  case class WaterSensor(id: String, ts: Long, vc: Int)

}

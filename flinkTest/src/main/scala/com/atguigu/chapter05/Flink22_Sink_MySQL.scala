package com.atguigu.chapter05

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
  *
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink22_Sink_MySQL {
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

    // 3.自定义SinkFunction，保存到MySQL
    mapDS.addSink(new MySinkFunction())

    // 5. 执行
    env.execute()
  }


  class MySinkFunction extends RichSinkFunction[WaterSensor] {

    var conn: Connection = _
    var pstmt: PreparedStatement = _

    /**
      * 做一个初始化的操作，比如创建连接对象
      * 每条数据执行的插入语句都一样，预编译对象没必要重复创建
      *
      * @param parameters
      */
    override def open(parameters: Configuration): Unit = {
      conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test","root","000000")
      pstmt = conn.prepareStatement("INSERT INTO sensor VALUES(?,?,?)")
    }

    override def invoke(value: WaterSensor, context: SinkFunction.Context[_]): Unit = {
      pstmt.setString(1, value.id)
      pstmt.setLong(2, value.ts)
      pstmt.setInt(3, value.vc)
      pstmt.execute()
    }

    override def close(): Unit = {
      pstmt.close()
      conn.close()
    }
  }


  // 定义样例类：水位传感器：用于接收空高数据
  // id:传感器编号
  // ts:时间戳
  // vc:空高 (为了方便，在本次课程当成水位高度来使用)
  case class WaterSensor(id: String, ts: Long, vc: Int)

}

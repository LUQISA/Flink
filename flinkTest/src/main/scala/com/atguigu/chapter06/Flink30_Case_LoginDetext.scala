package com.atguigu.chapter06



import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object Flink30_Case_LoginDetext {
  def main(args: Array[String]): Unit = {


    //1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //2.读取数据
    val loginDS: DataStream[LoginEvent] =
      env
        .readTextFile("input/LoginLog.csv")
        .map(
          line => {
            val datas: Array[String] = line.split(",")
            LoginEvent(
              datas(0).toLong,
              datas(1),
              datas(2),
              datas(3).toLong
            )
          }
        )  .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(10)) {
          override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
        }
      )



    //3.处理数据
    //1.如果2s内多次时报 10次 用liststate
    //2.代码中没有保证两次失败是连续的 中间的隔了成功
    //3.数据是乱序的 应该是F,S,F 但是数据来的顺序是 F ,F , S 那就不会误判了
    //3.1 过滤
    val filterDS: DataStream[LoginEvent] = loginDS.filter(_.eventType == "fail")
    //3.2 按照统计维度分类 用户
    val loginKS: KeyedStream[LoginEvent, Long] = filterDS.keyBy(_.userId)

    //3.3
    loginKS
      .process(
        new KeyedProcessFunction[Long, LoginEvent, String] {
          private var lastLoginFail: ValueState[LoginEvent] = _

          override def open(parameters: Configuration) = {
             lastLoginFail= getRuntimeContext.getState(new ValueStateDescriptor[LoginEvent]("lastLoginFail", classOf[LoginEvent]))
          }

          override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, String]#Context, out: Collector[String]) = {
            //如果登陆失败 判断是否是第一条
            if (value.eventType == "fail") {
              if (lastLoginFail.value() == null) {
                //说明不是第一条失败的数据 ,已经两次是失败了
                lastLoginFail.update(value)
              } else {
                //说明不是第一条失败的数据 一i经历过两次失败了
                if (Math.abs(value.eventTime - lastLoginFail.value().eventTime) <= 2) {
                  out.collect("用户" + value.userId + "在2s内连续2次登录失败 ! 可能为恶意登录")
                }
              }
            }
          }
        }
      )
      .print("login detect")

    //5.执行
    env.execute()
  }
    /**
      * 登陆行为样例类
      *
      * @param userId    用户ID
      * @param ip
      * @param eventType 事件类型：成功、失败
      * @param eventTime 时间时间
      */
    case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

}
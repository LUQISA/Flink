package com.atguigu.chapter06


import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/*
  订单支付超市监控
*/
object Flink31_Case_OrderTimeOut {
  def main(args: Array[String]): Unit = {

    //1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //2.读取数据
    val orderDS: DataStream[OrderEvent] = env
      .readTextFile("input/OrderLog.csv")
      .map(
        line => {
          val datas: Array[String] = line.split(",")
          OrderEvent(
            datas(0).toLong,
            datas(1),
            datas(2),
            datas(3).toLong
          )
        }
      )
      .assignAscendingTimestamps(_.eventTime * 1000L)


    //3 处理数据
    //3.1 按照统计的分维度 订单
     val orderKS: KeyedStream[OrderEvent, Long] = orderDS.keyBy(_.orderId)

    //3.2
    val resultDS: DataStream[String] = orderKS.process(
      new KeyedProcessFunction[Long, OrderEvent, String] {
        private var payEvent: ValueState[OrderEvent] = _
        private var createEvent: ValueState[OrderEvent] = _
        private var timeoutTs: ValueState[Long] = _

        val timestamp = new OutputTag[String]("timeout")


        override def open(parameters: Configuration) = {
          payEvent = getIterationRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("payEvent", classOf[OrderEvent]))
          createEvent = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("createEvent", classOf[OrderEvent]))
          timeoutTs = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timeoutTs", classOf[Long]))
        }

        //每个订单的第一条数据来的时候,应该注册一个定时器
        override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, String]#Context, out: Collector[String]) = {
          //不管来的是 creat 还是 pay 没必要作分区  直接登15分钟 不来 要么异常,要么超时
          //定时器的作用,就是发现只有一个数据来的情况
          if (timeoutTs.value() == 0) {
            ctx.timerService().registerEventTimeTimer(ctx.timerService().currentProcessingTime() + 15 * 60 * 1000L)
            timeoutTs.update(ctx.timerService().currentProcessingTime() + 15 * 60 * 1000L)
          } else {
            //表示来的不是本订单的第一天的数据 ,表示create和pay 到了 ,那么可以删除定时器 ,后面的逻辑有哦超时进行判断
            //ctx.timerService().deleteProcessingTimeTimer(timeoutTs.value())
            ctx.timerService().deleteEventTimeTimer(timeoutTs.value())
            timeoutTs.clear()
          }
          //来的数据可能是create .也可能是pay
          if (value.eventType == "create") {
            //1.来的数据是create
            //判断pay 来过没有
            if (payEvent.value() == null) {
              //1.1pay 没有来过 => 把create 保存起来,等待pay 的来临
              createEvent.update(value)
            } else {
              //1.2 pay 来过,判断一下是否超时
              if (payEvent.value().eventTime - value.eventTime > 15 * 60) {
                val timeoutTag = new OutputTag[String]("timeout")
                ctx.output(timeoutTag, "订单" + value.orderId + "支付成功,但是超时,请检查业务系统是否存在异常")
              } else {
                out.collect("订单" + value.orderId + "支付成功")
              }
              payEvent.clear()
            }
          } else {
            //2.来的数据是pay =>判断 create 来过没有
            if (createEvent.value() == null) {
              //2.1 create 没来过 => 把pay 保存起来
              payEvent.update(value)
            } else {
              //2.2 create 已经来了
              if (value.eventTime - createEvent.value().eventTime > 15 * 60) {
                ctx.output(timeoutTag, "订单" + value.orderId + "支付成功, 但是超时 ,前检查业务系统是否存在异常")
              } else {
                out.collect("订单" + value.orderId + " 支付成功!")
              }
            }
            createEvent.clear()
          }
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, String]#OnTimerContext, out: Collector[String]) = {
          //判断是谁没来
          if (payEvent.value() != null) {
            //1. pay 来过 ,create 没来
            ctx.output(timeoutTag, "订单" + createEvent.value().orderId + "超时,用户未支付")
            createEvent.clear()
          }
          if(createEvent.value() !=null){
            //2.create 来过 ,pay没来
            ctx.output(timeoutTag,"订单" + createEvent.value().orderId + "超时,用户未支付")
            createEvent.clear()

          }

          //清空保存的定时时间
          timeoutTs.clear()
        }
      }
    )
    resultDS
    val timeoutTag = new OutputTag[String]("timeout")
    resultDS.getSideOutput(timeoutTag).print("timeout")
    resultDS.print("result")


  }

  /**
    * 订单数据样例类
    *
    * @param orderId   订单ID
    * @param eventType 时间类型：创建、支付
    * @param txId      交易码
    * @param eventTime 事件时间
    */
  case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)




}

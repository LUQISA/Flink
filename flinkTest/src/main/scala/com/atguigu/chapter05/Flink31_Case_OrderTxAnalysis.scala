package com.atguigu.chapter05

import com.atguigu.chapter05.Flink31_Case_OrderTxAnalysis.TxEvent
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  * 业务系统 和 交易系统 实时对账
  *
  * @version 1.0
  * @author create by cjp on 2020/8/31 9:31
  */
object Flink31_Case_OrderTxAnalysis {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    // 2.分别读取 订单系统 和 交易系统 两条流,转换成样例类
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
    val txDS: DataStream[TxEvent] = env
      .readTextFile("input/ReceiptLog.csv")
      .map(
        line => {
          val datas: Array[String] = line.split(",")
          TxEvent(
            datas(0),
            datas(1),
            datas(2).toLong
          )
        }
      )

    // 3.通过connect将两条流连接在一起\
    // TODO 两条流进行connect，最好是做 keyBy处理，避免 因数据不在一起 造成的匹配不上
    // 如果两条流的 key相同，会放到一起
    // 可以先 keyBy，再connect
    // 也可以先connect，再keyby
    val connectCS: ConnectedStreams[OrderEvent, TxEvent] = orderDS.connect(txDS)
//    val connectCS: ConnectedStreams[OrderEvent, TxEvent] = (orderDS.keyBy(_.txId)).connect(txDS.keyBy(_.txId))

    // 4.通过交易码做一个对比
    val processDS: DataStream[String] = connectCS
      .keyBy(_.txId,_.txId)
      .process(new MyCoProcessFunction())

    // 5.打印
    processDS.print("order tx")

    // 6.执行
    env.execute()

  }

  class MyCoProcessFunction extends CoProcessFunction[OrderEvent, TxEvent, String] {

    // 用来临时保存 交易数据 ，key是 交易码， value是 完整数据
    val txMap = new mutable.HashMap[String, TxEvent]()
    // 用来临时保存 订单数据 ，key是 交易码， value是 完整数据
    val orderMap = new mutable.HashMap[String, OrderEvent]()

    /**
      * 处理订单系统的数据：来一条处理一条
      *
      * @param value
      * @param ctx
      * @param out
      */
    override def processElement1(value: OrderEvent, ctx: CoProcessFunction[OrderEvent, TxEvent, String]#Context, out: Collector[String]): Unit = {
      // 进到这个方法，表示当前来的数据是 订单系统 的数据
      // 要和 交易数据 产生联系，需要保存一个变量里，两条流的数据，不一定谁先来，所以首先要查一下这个变量里的数据
      val maybeTxEvent: Option[TxEvent] = txMap.get(value.txId)
      // 1.判断 交易系统 的数据来了没有
      if (maybeTxEvent.isEmpty) {
        // 1.1 交易系统 的数据还没来，那么把自己存起来
        orderMap.put(value.txId, value)
      } else {
        // 1.2 交易系统 的数据已经来过了,对账成功
        out.collect("订单[" + value.orderId + "]对账成功")
        // 对账成功，删除保存的数据
        txMap.remove(value.txId)
      }

    }

    /**
      * 处理交易系统的数据：来一条处理一条
      *
      * @param value
      * @param ctx
      * @param out
      */
    override def processElement2(value: TxEvent, ctx: CoProcessFunction[OrderEvent, TxEvent, String]#Context, out: Collector[String]): Unit = {
      // 进到这个方法，表示当前来的数据是 交易系统 的数据
      // 要和 订单数据 产生联系，需要保存一个变量里，两条流的数据，不一定谁先来，所以首先要查一下这个变量里的数据
      val maybeOrderEvent: Option[OrderEvent] = orderMap.get(value.txId)
      // 1.判断 订单系统 的数据来了没有
      if (maybeOrderEvent.isEmpty) {
        // 1.1 订单系统 的数据还没来，那么把自己存起来
        txMap.put(value.txId, value)
      } else {
        // 1.2 订单系统 的数据已经来过了,对账成功
        out.collect("订单[" + maybeOrderEvent.get.orderId + "]对账成功")
        // 对账成功，删除保存的数据
        orderMap.remove(value.txId)
      }
    }

  }


  /**
    * 订单系统数据的样例类
    *
    * @param orderId   订单ID
    * @param eventType 时间类型：下单、支付
    * @param txId      交易码：用来唯一标识某一笔交易，用来与交易系统连接
    * @param eventTime 事件时间：数据产生的时间
    */
  case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)

  /**
    * 交易系统数据的样例类
    *
    * @param txId       交易码：用来唯一标识某一笔交易，用来与订单系统连接
    * @param payChannel 支付渠道：微信、支付宝
    * @param eventTime  事件时间：数据产生的时间
    */
  case class TxEvent(txId: String, payChannel: String, eventTime: Long)


}

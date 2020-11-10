package com.atguigu.chapter06

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


/**
  *
  *热门页面浏览量
  *   每5秒 输出 最近10分钟内的 商品点击最多的前N个url
  *
  *
  * @version 1.0
  * @author create by cjp on 2020/8/28 11:30
  */
object Flink27_Case_HotltemPagViewAnalysis {
  def main(args: Array[String]): Unit = {

    // 1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2.读取数据
    val logDS: DataStream[String] = env.readTextFile("input/apache.log")
    // 转换成样例类
    val apacheLogDS: DataStream[ApacheLog] = logDS
      .map(
        line => {
        val datas: Array[String] = line.split(" ")
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val ts: Long = sdf.parse(datas(3)).getTime
        ApacheLog(
          datas(0),
          datas(1),
          ts,
          datas(5),
          datas(6)
        )
      }
    )
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[ApacheLog](Time.minutes(1)) {
          override def extractTimestamp(element: ApacheLog): Long = element.eventTime
        }
      )
    // 3.处理数据:

    //3.1按照:统计维度 url  分组
    val apacheLogKS: KeyedStream[ApacheLog, String] = apacheLogDS.keyBy(_.url)
    //3.2 开窗每隔5秒输出最近10分钟内
    val apacheLogWS: WindowedStream[ApacheLog, String, TimeWindow] = apacheLogKS.timeWindow(Time.minutes(10), Time.seconds(5))
    //3.3 聚合操作
    // 第一个函数 做预聚合操作 结果传递给全窗口函数
    // 第二个函数 将预聚合结果   加上窗口结束时间 的标记 ,方便后面 按窗口进行分组
    val aggDS: DataStream[HotPageView] = apacheLogWS.aggregate(
      new SimplePreAggregateFunction[ApacheLog](),
      new MyProcessWindowFunction()
    )
    //  3.4按照 窗口的结束时间 分组 :同来自于 同一个窗口的数据 ,放在一起 方便进行排序
    val aggKS: KeyedStream[HotPageView, Long] = aggDS.keyBy(_.windowEnd)
    // 3.5 进行排序
    val resultDS: DataStream[String] = aggKS.process(
      new KeyedProcessFunction[Long, HotPageView, String] {

        private var dataList: ListState[HotPageView] = _
        private var triggerTs: ValueState[Long] = _

        override def open(parameters: Configuration) = {
          dataList = getRuntimeContext.getListState(new ListStateDescriptor[HotPageView]("dataList", classOf[HotPageView]))
          triggerTs = getRuntimeContext.getState(new ValueStateDescriptor[Long]("triggerTs", classOf[Long]))
        }

        override def processElement(value: HotPageView, ctx: KeyedProcessFunction[Long, HotPageView, String]#Context,  out: Collector[String]) = {
          //1.存数据
          dataList.add(value)
          //2.用定时器,模拟窗口的触发操作
          if (triggerTs.value() == 0) {
            ctx.timerService().registerEventTimeTimer(value.windowEnd)
            triggerTs.update(value.windowEnd)
          }
        }

        /*
            定时器触发操作: 排序 => 取前
         */
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, HotPageView, String]#OnTimerContext, out: Collector[String]) = {

          //1.从集合取出
          val datas: util.Iterator[HotPageView] = dataList.get().iterator()
          //2. 放入scala 的集合
          val listBuffer = new ListBuffer[HotPageView]
          while (datas.hasNext) {
            listBuffer.append(datas.next())
          }
          //3.排序 取前3
          val top3: ListBuffer[HotPageView] = listBuffer.sortBy(_.clickCount)(Ordering.Long.reverse).take(3)
          //4.通过采集器发送到下游
          out.collect(
            s"""
               |窗口结束时间:${new Timestamp(timestamp)}
               | ++++++++++++++++++++++++++++++++++++++
               |${top3.mkString("\n")}
               |======================================
            """.stripMargin
          )
        }
      }
    )

    //4.打印
    resultDS.print("top3 hot page")
    // 5. 执行
    env.execute()
  }




  /*
        全窗口函数: 给每个窗口的统计结果,假声 窗口结束时间 的标记
        输入 时预聚合函数的输入 也是Long
   */
  class MyProcessWindowFunction extends ProcessWindowFunction[Long,HotPageView,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[HotPageView]): Unit = {
     out.collect(HotPageView(key,elements.iterator.next(),context.window.getEnd))

    }
  }
  case class HotPageView(url:String,clickCount:Long,windowEnd:Long)





  /*
        简单的预聚合函数
   */
class  SimplePreAggregateFunction[T] extends AggregateFunction[T,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(value: T, accumulator: Long): Long = accumulator+1L

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a+b
  }





  /*
    页面浏览的样例类

   */

  case class ApacheLog(
                      IP:String,
                      userId: String,
                      eventTime: Long,
                      method : String,
                      url: String)

}

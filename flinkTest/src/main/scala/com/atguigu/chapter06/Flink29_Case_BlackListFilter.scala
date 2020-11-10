package com.atguigu.chapter06


import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/*
    统计 各个省份的每个广告的点击量
 */
object Flink29_Case_BlackListFilter {
  def main(args: Array[String]): Unit = {

    //1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //并行度设置
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //2.读取数据
    val adClickDS: DataStream[AdClickLog] = env
      .readTextFile("input/AdClickLog.csv")
      .map(
        line => {
          val datas: Array[String] = line.split(",")
          AdClickLog(
            datas(0).toLong,
            datas(1).toLong,
            datas(2),
            datas(3),
            datas(4).toLong
          )
        }
      )
     .assignAscendingTimestamps(_.timestamp* 1000L)

    //3.处理数据
    //3.1按照统计的维度进行分组 (维度 ;省份,广告) ,多个维度可以拼接在一起

   val  provinceAndAdAndOneKS: KeyedStream[AdClickLog, (Long, Long)] = adClickDS
    .keyBy(data => (data.userId, data.adId))

    //3.2 进行黑名单过滤
    val blackFilterDS: DataStream[AdClickLog] = provinceAndAdAndOneKS.process(new BlackListFilter())
    val alarmTag = new OutputTag[String]("alarm")
    blackFilterDS.getSideOutput(alarmTag).print("blacklist")


    //3.3正常的业务处理逻辑 比如 topN
    blackFilterDS
        .keyBy(data => (data.userId,data.adId))
        .timeWindow(Time.hours(1),Time.minutes(5))
        .aggregate(
          new SimplePreAggregateFunction[AdClickLog](),
          new ProcessWindowFunction[Long,HotAdClick,(Long,Long),TimeWindow] {
            override def process(key: (Long, Long), context: Context, elements: Iterable[Long], out: Collector[HotAdClick]): Unit = {
              out.collect(HotAdClick(key._1,key._2,elements.iterator.next(),context.window.getEnd))
            }
          }
        )
      .keyBy(_.windowEnd)
        .process(
          new KeyedProcessFunction[Long,HotAdClick,String] {
            override def processElement(value: HotAdClick, ctx: KeyedProcessFunction[Long, HotAdClick, String]#Context, out: Collector[String]) = {
              out.collect("用户" + value.userId + "对广告" + "点击了" + value.clickCount + "次")
            }
          }
        )



    //5.执行
    env.execute()
  }

  class BlackListFilter extends  KeyedProcessFunction[(Long,Long),AdClickLog,AdClickLog]{
    private  var clickCount:ValueState[Int]=_
    private  var timeTs : Long =0L
    private  var alarmFlag : ValueState[Boolean]=_


    override def open(parameters: Configuration): Unit = {
      clickCount = getRuntimeContext.getState(new ValueStateDescriptor[Int]("clickCount",classOf[Int]))
      alarmFlag = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("alarmFlag",classOf[Boolean]))
    }


    override def processElement(value: AdClickLog,ctx : KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
      val currentCount: Int = clickCount.value()

      //如果点击量 =0 说明今天第一次点击 也就是说 这条数据属于今天
      if(currentCount ==0){
        //考虑隔天点 对count 值清零 => 用定时器来实现 =>需要获取隔天的 0点的时间戳
        //1.取整 => 去当天的0点的时间戳 对应的天数 2020-09-04 10:18:50 => 2020-09-04 00:00:00 对应的天数
        val currentDay: Long = value.timestamp * 1000L/(24*60*60*1000L)
        //2.隔天的天数 =当前天数 +1
        val nextDay: Long = currentDay+1
        //3.隔天的天数 转换成时间戳
        val nextDayStartTs: Long = nextDay *(24*60*60*1000L)

        //注册隔天 0点的定时器 做了判断 防止不同用户的数据都来注册定时器 如果日活很大 那么重复创建的次数很多
        if(timeTs ==0){
          ctx.timerService().registerEventTimeTimer(nextDayStartTs)
          timeTs=nextDayStartTs
        }

        if (currentCount >=100){
          //如果点击量超过阀值100 那么就 侧输出流 写入告警信息
          //如果还没告警 就告警 告警过了之后,不在告警
          if(!alarmFlag.value()) {
            //boolean默认值就是false 那么进入到这里,说明还没告警
            //首次告警,正常输出到侧输出流

            val alarmTag = new OutputTag[String]("alarm")
            ctx.output(alarmTag, "用户" + value.userId + "对广告" + value.adId + "今日内点击达到阀值,可能为恶意刷单")
            alarmFlag.update(true)
          }
        }else
          {
            //没达到阀值 那么就正常统计点击量 并且往下游传递数据
            clickCount.update(currentCount +1)

            //out.collect(""用户 + value.userId + "对广告" + value.adId + "点击了" + clickCount.value() + "次" )
            out.collect(value)

          }
      }
    }

    /*
       定时器的触发 ;说明已经隔天到了 0点 要 清空count值

      */
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
      clickCount.clear()
      timeTs =0L
      alarmFlag.clear()
    }
  }



  case class HotAdClick (userId: Long,adId:Long,clickCount:Long,windowEnd:Long)

  /*
      广告点击样例类  用户ID
      userid     广告ID
      adid       省份
      province   城市
      timestamp  时间戳
   */
  case class AdClickLog(
                         userId: Long,
                          adId: Long,
                          province: String,
                          city: String,
                          timestamp: Long)



  class  SimplePreAggregateFunction[T] extends AggregateFunction[T,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(value: T, accumulator: Long): Long =accumulator + 1L

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }




}

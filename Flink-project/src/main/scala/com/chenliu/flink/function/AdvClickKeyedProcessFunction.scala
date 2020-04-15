package com.chenliu.flink.function

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

//K, I, O
class AdvClickKeyedProcessFunction extends KeyedProcessFunction[String, (String, Long), (String, Long)] {

  private var advClickCount: ValueState[Long] = _

  private var flag: ValueState[Boolean] = _

  override def open(parameters: Configuration): Unit = {
    advClickCount = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("advClickCount", classOf[Long])
    )
    flag = getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("flag", classOf[Boolean])
    )
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, Long), (String, Long)]#OnTimerContext, out: Collector[(String, Long)]): Unit = {
    advClickCount.clear()
    flag.clear()
  }

  override def processElement(value: (String, Long), ctx: KeyedProcessFunction[String, (String, Long), (String, Long)]#Context, out: Collector[(String, Long)]): Unit = {
    //增加天的概念
    //当第二天的时候，将前一天的状态清空，重新计算

    val count: Long = advClickCount.value()

    //如何触发定时器
    //1.取得第一条数据的处理时间,然后计算出第二天的时间
    if (count == 0) {
      val currentTime: Long = ctx.timerService().currentProcessingTime()
      val dayTime = currentTime / (1000 * 60 * 60 * 24)
      val nextDayTime = currentTime + 1
      val nexDayTimeStamp = nextDayTime * (1000 * 60 * 60 * 24)
      //根据处理时间计算得出第二天的起始时间，重置状态，触发定时器
      ctx.timerService().registerProcessingTimeTimer(nexDayTimeStamp)
    }

    val latestCount = count + 1

    if (latestCount >= 100) {
      if (!flag.value()) {
        val outputTag = new OutputTag[(String, Long)]("blackList")
        ctx.output(outputTag, value)
        //更新提示状态
        flag.update(true)
      }

    } else {
      out.collect(value)
    }

    advClickCount.update(latestCount)

  }
}

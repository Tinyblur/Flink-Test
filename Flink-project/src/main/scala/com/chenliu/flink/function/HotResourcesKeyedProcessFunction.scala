package com.chenliu.flink.function

import java.lang
import java.sql.Timestamp

import com.chenliu.flink.bean.HotResourceClick
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

//K, I, O
class HotResourcesKeyedProcessFunction  extends KeyedProcessFunction[Long,HotResourceClick,String]{

  private var resourceList:ListState[HotResourceClick] = _

  private var alarmTimer:ValueState[Long] = _

  override def open(parameters: Configuration): Unit = {
    resourceList = getRuntimeContext.getListState(
      new ListStateDescriptor[HotResourceClick]("resourceList",classOf[HotResourceClick])
    )
    alarmTimer = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("alarmTimer",classOf[Long])
    )
  }

  //触发排序，输出
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, HotResourceClick, String]#OnTimerContext, out: Collector[String]): Unit = {
    val iterable: lang.Iterable[HotResourceClick] = resourceList.get()
    val list = new ListBuffer[HotResourceClick]
    import scala.collection.JavaConversions._
    for (data <- iterable){
      list.add(data)
    }

    //清除状态
    resourceList.clear()
    alarmTimer.clear()

    val result: ListBuffer[HotResourceClick] = list.sortWith(
      (a, b) => {
        a.clickCount > b.clickCount
      }
    ).take(3)

    //将结果输出到控制台
    val builder = new StringBuilder
    builder.append("当前时间：" + new Timestamp(timestamp) + "\n")
    for (elem <- result) {

      builder.append("url：" + elem.url + ",点击数量：" + elem.clickCount + "\n")

    }

    builder.append("====================")

    out.collect(builder.toString())

    Thread.sleep(1000)
  }

  override def processElement(value: HotResourceClick, ctx: KeyedProcessFunction[Long, HotResourceClick, String]#Context, out: Collector[String]): Unit = {

    //保存数据的状态
    resourceList.add(value)
    //设置定时器
    if (alarmTimer.value() == 0){
      ctx.timerService().registerEventTimeTimer(value.windowEndTime)
      alarmTimer.update(value.windowEndTime)
    }

  }
}

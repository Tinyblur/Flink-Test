package com.chenliu.flink.function

import java.sql.Timestamp
import java.{lang, util}

import com.chenliu.flink.bean.HotItemClick
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * 热门商品的处理函数
  */
class HotItemProcessFunction extends KeyedProcessFunction[Long, HotItemClick, String] {

  //数据集合
  private var itemListState: ListState[HotItemClick] = _
  //定时器
  private var alartTimer:ValueState[Long] = _

  //对状态变量进行初始化赋值
  override def open(parameters: Configuration): Unit = {
      itemListState = getRuntimeContext.getListState(
        new ListStateDescriptor[HotItemClick]("itemListState",classOf[HotItemClick])
      )

    alartTimer = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("alartTimer",classOf[Long])
    )
  }

  //TODO 2.当定时器触发时，进行数据排序输出
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, HotItemClick, String]#OnTimerContext, out: Collector[String]): Unit = {
    val datas: lang.Iterable[HotItemClick] = itemListState.get()
    val iterData: util.Iterator[HotItemClick] = datas.iterator()
    val list = new ListBuffer[HotItemClick]
    while (iterData.hasNext){
      list.append(iterData.next())
    }

    //将状态数据清楚
    itemListState.clear()
    alartTimer.clear()

    //对集合中的数据进行排序
    val result: ListBuffer[HotItemClick] = list.sortBy(_.clickCount)(Ordering.Long.reverse).take(3)

    //将结果输出到控制台
    val builder = new StringBuilder
    builder.append("当前时间：" + new Timestamp(timestamp) + "\n")
    for (elem <- result) {

      builder.append("商品：" + elem.itemId + ",点击数量：" + elem.clickCount + "\n")

    }

    builder.append("====================")

    out.collect(builder.toString())

    Thread.sleep(1000)

  }

  override def processElement(value: HotItemClick, ctx: KeyedProcessFunction[Long, HotItemClick, String]#Context, out: Collector[String]): Unit = {

    //TODO 1.将每一条数据保存起来,设定定时器
    itemListState.add(value)
    if (alartTimer.value() == 0){
      ctx.timerService().registerEventTimeTimer(value.windowEndTime)
      alartTimer.update(value.windowEndTime)
    }

  }
}

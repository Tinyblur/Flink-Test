package com.chenliu.flink.function

import com.chenliu.flink.bean.{HotItemClick, HotResourceClick}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

//IN, OUT, KEY, W <: Window
class HotResourcesWindowFunction extends WindowFunction[Long,HotResourceClick,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[HotResourceClick]): Unit = {
    out.collect(HotResourceClick(key,input.iterator.next(),window.getEnd))
  }
}

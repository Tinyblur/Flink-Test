package com.chenliu.flink.function

import com.chenliu.flink.bean.ApacheLog
import org.apache.flink.api.common.functions.AggregateFunction

//输入，中间，输出类型
class HotResourcesAggregateFunction extends AggregateFunction[ApacheLog, Long, Long] {
  override def add(value: ApacheLog, accumulator: Long): Long = {
    accumulator + 1L
  }
  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

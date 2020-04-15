package com.chenliu.flink.function

import com.chenliu.flink.bean.UserBehavior
import org.apache.flink.api.common.functions.AggregateFunction

class HotItemAggregateFunction extends AggregateFunction[UserBehavior,Long,Long]{
  override def add(value: UserBehavior, accumulator: Long): Long = {
    accumulator + 1L
  }

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

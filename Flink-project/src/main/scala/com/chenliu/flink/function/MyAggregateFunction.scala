package com.chenliu.flink.function

import org.apache.flink.api.common.functions.AggregateFunction

class MyAggregateFunction[T] extends AggregateFunction[T,Long,Long]{
  override def add(value: T, accumulator: Long): Long = {
    accumulator + 1L
  }

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

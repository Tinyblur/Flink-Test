package com.chenliu.flink

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._


object Flink_Transform_Map {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

//    val collectDS: DataStream[Int] = env.fromCollection(List(2,90,78,35))

//    val value: DataStream[Int] = collectDS.map(new MyMapFunction)

    val collectDS1: DataStream[String] = env.fromCollection(List("a","b","c"))

    collectDS1.map(new MyMapFunction_java()).print("java")

//    value.print("myMapFunction")

    env.execute()

  }

  class MyMapFunction extends MapFunction[Int,Int]{
    override def map(value: Int): Int = {
      value * 2
    }
  }

}

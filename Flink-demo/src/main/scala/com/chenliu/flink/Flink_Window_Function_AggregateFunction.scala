package com.chenliu.flink

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._

object Flink_Window_Function_AggregateFunction {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputDS: DataStream[String] = env.socketTextStream("hadoop102",9999)

    val wordDS: DataStream[String] = inputDS.flatMap(_.split(" "))

    val valueDS: DataStream[Int] = wordDS.map((_, 1)).keyBy(_._1).countWindow(3).aggregate(
      //定义泛型，输入，中间处理值，输出
      new AggregateFunction[(String, Int), Int, Int]() {
        override def add(value: (String, Int), accumulator: Int): Int = {
          //根据输入值对累加器的值进行更新
          accumulator + value._2
        }

        //创建累加器
        override def createAccumulator(): Int = 0

        //获取结果数据
        override def getResult(accumulator: Int): Int = accumulator

        override def merge(a: Int, b: Int): Int = a + b
      }
    )

    valueDS.print("TimeWindow")

    env.execute()

  }
}

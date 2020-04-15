package com.chenliu.flink

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object Flink_Window_Function_ReduceFunction {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputDS: DataStream[String] = env.socketTextStream("hadoop102",9999)

    val wordDS: DataStream[String] = inputDS.flatMap(_.split(" "))

    val valueDS: DataStream[(String, Int)] = wordDS.map((_, 1)).keyBy(_._1).timeWindow(Time.seconds(3)).reduce(
      new ReduceFunction[(String,Int)]() {
        override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
          (value1._1,value1._2+value2._2)
        }
      }
    )

    valueDS.print("TimeWindow")

    env.execute()

  }
}
